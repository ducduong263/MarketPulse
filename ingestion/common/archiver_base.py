"""
ingestion/common/archiver_base.py

Base class for all Kafka Avro -> Delta Lake (MinIO/S3) archivers.

Handles:
- MinIO bucket creation (boto3)
- Kafka DeserializingConsumer setup
- PyArrow table construction and write_deltalake
- Flush with exponential-backoff retry
- Explicit Kafka offset commit after Delta write (at-least-once)
- Graceful shutdown (SIGINT/SIGTERM)
"""
from __future__ import annotations

import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import boto3
import pyarrow as pa
from botocore.exceptions import ClientError
from confluent_kafka import DeserializingConsumer, KafkaError, Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from deltalake import write_deltalake


def _build_storage_options() -> dict:
    """Build MinIO/S3 storage options dict from environment variables."""
    endpoint = os.getenv("minio_endpoint", "localhost:9000")
    return {
        "AWS_ENDPOINT_URL":          f"http://{endpoint}",
        "AWS_ACCESS_KEY_ID":         os.getenv("minio_root_user", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY":     os.getenv("minio_root_password", "minioadmin"),
        "AWS_REGION":                "us-east-1",
        "AWS_ALLOW_HTTP":            "true",
        "AWS_FORCE_PATH_STYLE":      "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME":"true",
    }


class DeltaLakeArchiver:
    """
    Reusable Kafka Avro -> Delta Lake archiver.

    Each concrete archiver provides:
      - topic:               Kafka topic name
      - consumer_group:      Consumer group ID
      - schema_path:         Path to .avsc schema file
      - delta_table_uri:     e.g. "s3://market-data/bronze/market_trade"
      - arrow_schema:        pa.Schema matching _record_to_row output
      - record_to_row_fn:    Function (record: dict, msg: Message) -> dict
      - flush_interval:      Seconds between flushes (default 300)
      - flush_size:          Records before forced flush (default 5_000)
      - max_retry:           Flush retries on error (default 3)
      - storage_options:     Override S3/MinIO options (default: from env)
      - partition_by:        Delta partition columns (default ["date"])
      - minio_bucket:        Bucket name to ensure exists (default "market-data")

    Usage:
        DeltaLakeArchiver(
            topic=KAFKA_TOPIC,
            consumer_group=CONSUMER_GROUP,
            schema_path=SCHEMA_PATH,
            delta_table_uri=DELTA_TABLE_URI,
            arrow_schema=ARROW_SCHEMA,
            record_to_row_fn=_record_to_row,
        ).run()
    """

    def __init__(
        self,
        topic: str,
        consumer_group: str,
        schema_path: Path,
        delta_table_uri: str,
        arrow_schema: pa.Schema,
        record_to_row_fn: Callable,
        flush_interval: int = 300,
        flush_size: int = 5_000,
        max_retry: int = 3,
        storage_options: dict | None = None,
        partition_by: list[str] | None = None,
        minio_bucket: str = "market-data",
    ) -> None:
        self.topic = topic
        self.consumer_group = consumer_group
        self._schema_path = schema_path
        self._delta_table_uri = delta_table_uri
        self._arrow_schema = arrow_schema
        self._record_to_row_fn = record_to_row_fn
        self._flush_interval = flush_interval
        self._flush_size = flush_size
        self._max_retry = max_retry
        self._storage_options = storage_options or _build_storage_options()
        self._partition_by = partition_by or ["date"]
        self._minio_bucket = minio_bucket

    # ── Public API ────────────────────────────────────────────────

    def run(self) -> None:
        """
        Full archiver lifecycle:
          1. Ensure MinIO bucket exists
          2. Create Kafka consumer + subscribe
          3. Poll + buffer messages
          4. Flush buffer -> Delta Lake (write_deltalake)
          5. Commit Kafka offset AFTER Delta write (at-least-once)
          6. Retry with exponential backoff on flush error
          7. Graceful shutdown + final flush
        """
        self._ensure_bucket()
        consumer = self._create_consumer()
        consumer.subscribe([self.topic])

        buffer: list[dict] = []
        last_buffered_msg: Message | None = None
        last_flush = time.monotonic()
        total = 0
        running = True

        def _stop(sig, frame):
            nonlocal running
            print("\n[STOP] Shutting down...")
            running = False

        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)

        print(f"[START] Consumer group: {self.consumer_group}")
        print(f"[CONFIG] {self.topic} -> Delta Lake {self._delta_table_uri}")
        print(f"[CONFIG] Flush every {self._flush_interval}s or {self._flush_size} records")

        try:
            while running:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    pass
                else:
                    err = msg.error()
                    if err:
                        if err.code() != KafkaError._PARTITION_EOF:
                            print(f"[ERROR] Kafka: {err}")
                    else:
                        record = msg.value()
                        if isinstance(record, dict):
                            buffer.append(self._record_to_row_fn(record, msg))
                            last_buffered_msg = msg

                now = time.monotonic()
                should_flush = (
                    len(buffer) >= self._flush_size or
                    (len(buffer) > 0 and now - last_flush >= self._flush_interval)
                )

                if should_flush and last_buffered_msg is not None:
                    for attempt in range(self._max_retry):
                        try:
                            total += self._flush(consumer, buffer, last_buffered_msg)
                            buffer.clear()
                            last_buffered_msg = None
                            last_flush = now
                            break
                        except Exception as e:
                            print(f"[WARN] Flush attempt {attempt + 1}/{self._max_retry}: {e}")
                            time.sleep(2 ** attempt)
                    else:
                        print(f"[ERROR] Flush failed after {self._max_retry} attempts — offset NOT committed")
                        buffer.clear()
                        last_buffered_msg = None
                        last_flush = now

        finally:
            if buffer and last_buffered_msg is not None:
                try:
                    total += self._flush(consumer, buffer, last_buffered_msg)
                except Exception as e:
                    print(f"[ERROR] Final flush failed: {e}")
            consumer.close()
            print(f"[DONE] Total archived: {total} records")

    # ── Internal ──────────────────────────────────────────────────

    def _ensure_bucket(self) -> None:
        """Create the MinIO bucket if it doesn't exist (delta-rs doesn't auto-create)."""
        endpoint = os.getenv("minio_endpoint", "localhost:9000")
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{endpoint}",
            aws_access_key_id=os.getenv("minio_root_user", "minioadmin"),
            aws_secret_access_key=os.getenv("minio_root_password", "minioadmin"),
            region_name="us-east-1",
        )
        try:
            s3.head_bucket(Bucket=self._minio_bucket)
        except ClientError:
            s3.create_bucket(Bucket=self._minio_bucket)
            print(f"[MINIO] Created bucket: {self._minio_bucket}")

    def _create_consumer(self) -> DeserializingConsumer:
        schema_str = self._schema_path.read_text(encoding="utf-8")
        sr_client = SchemaRegistryClient({
            "url": os.getenv("schema_registry_url", "http://localhost:8081")
        })
        avro_deserializer = AvroDeserializer(sr_client, schema_str)
        return DeserializingConsumer({
            "bootstrap.servers":  os.getenv("kafka_bootstrap_servers", "localhost:9092"),
            "group.id":           self.consumer_group,
            "auto.offset.reset":  "earliest",
            "value.deserializer": avro_deserializer,
            "enable.auto.commit": False,
        })

    def _flush(self, consumer: DeserializingConsumer, buffer: list, last_msg: Message) -> int:
        """Write buffer to Delta Lake then commit Kafka offset (at-least-once)."""
        if not buffer:
            return 0

        table = pa.Table.from_pylist(buffer, schema=self._arrow_schema)
        write_deltalake(
            self._delta_table_uri,
            table,
            mode="append",
            partition_by=self._partition_by,
            storage_options=self._storage_options,
            schema_mode="merge",
        )
        # Commit AFTER Delta write — ensures at-least-once delivery
        consumer.commit(message=last_msg)

        n = len(buffer)
        date_val = buffer[0].get("date", "?")
        print(f"[FLUSH] {n} records -> {self._delta_table_uri} (date={date_val}, offset={last_msg.offset()})")
        return n
