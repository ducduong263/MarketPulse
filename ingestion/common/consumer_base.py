"""
ingestion/common/consumer_base.py

Base class for all Kafka Avro -> TimescaleDB consumers.

Handles:
- Kafka DeserializingConsumer setup (Avro + Schema Registry)
- PostgreSQL connection via psycopg2
- Batch insert with execute_values
- Graceful shutdown (SIGINT/SIGTERM)
- Optional board_id filtering (ALLOWED_BOARDS)
"""
from __future__ import annotations

import os
import signal
import time
from pathlib import Path
from typing import Callable

import psycopg2
import psycopg2.extras
from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from .avro_utils import unwrap_union, ms_to_ts  # noqa: F401 — re-exported for consumers


class KafkaTimescaleConsumer:
    """
    Reusable Kafka Avro -> TimescaleDB batch consumer.

    Each concrete consumer provides:
      - topic:             Kafka topic name
      - consumer_group:    Consumer group ID
      - schema_path:       Path to .avsc schema file
      - insert_sql:        SQL with VALUES %s placeholder
      - record_to_row_fn:  Function (record: dict) -> tuple for DB insert
      - batch_size:        Flush when batch reaches this size (default 100)
      - batch_timeout:     Flush when batch age exceeds this (seconds, default 2.0)
      - allowed_boards:    Optional set of board_id strings to allow (None = no filter)

    Usage:
        KafkaTimescaleConsumer(
            topic=KAFKA_TOPIC,
            consumer_group=CONSUMER_GROUP,
            schema_path=SCHEMA_PATH,
            insert_sql=INSERT_SQL,
            record_to_row_fn=_record_to_row,
            batch_size=100,
            batch_timeout=2.0,
            allowed_boards={"G1"},
        ).run()
    """

    def __init__(
        self,
        topic: str,
        consumer_group: str,
        schema_path: Path,
        insert_sql: str,
        record_to_row_fn: Callable,
        batch_size: int = 100,
        batch_timeout: float = 2.0,
        allowed_boards: set | None = None,
    ) -> None:
        self.topic = topic
        self.consumer_group = consumer_group
        self._schema_path = schema_path
        self._insert_sql = insert_sql
        self._record_to_row_fn = record_to_row_fn
        self._batch_size = batch_size
        self._batch_timeout = batch_timeout
        self._allowed_boards = allowed_boards  # None means accept all boards

    # ── Public API ────────────────────────────────────────────────

    def run(self) -> None:
        """
        Full consumer lifecycle:
          1. Create Kafka consumer + DB connection
          2. Poll + batch messages
          3. Flush batch to TimescaleDB (execute_values)
          4. Commit Kafka offset after successful DB commit
          5. Graceful shutdown on SIGINT/SIGTERM + final flush
        """
        consumer = self._create_consumer()
        conn = self._create_db_conn()
        consumer.subscribe([self.topic])

        batch: list[tuple] = []
        last_flush = time.monotonic()
        total_rows = 0
        skipped = 0

        print(f"[START] Consumer group: {self.consumer_group}")
        print(f"[CONFIG] Topic: {self.topic} | Batch: {self._batch_size} | Timeout: {self._batch_timeout}s")
        if self._allowed_boards:
            print(f"[CONFIG] Board filter: {self._allowed_boards}")

        running = True

        def _signal_handler(sig, frame):
            nonlocal running
            print("\n[STOP] Shutting down consumer...")
            running = False

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        try:
            while running:
                msg = consumer.poll(timeout=0.5)

                if msg is None:
                    pass
                else:
                    err = msg.error()
                    if err:
                        if err.code() != KafkaError._PARTITION_EOF:
                            print(f"[ERROR] Kafka error: {err}")
                    else:
                        record = msg.value()
                        if isinstance(record, dict):
                            if self._allowed_boards is not None:
                                board = record.get("board_id", "")
                                if board not in self._allowed_boards:
                                    skipped += 1
                                    continue
                            batch.append(self._record_to_row_fn(record))

                now = time.monotonic()
                should_flush = (
                    len(batch) >= self._batch_size or
                    (len(batch) > 0 and now - last_flush >= self._batch_timeout)
                )

                if should_flush:
                    self._flush_batch(conn, consumer, batch, total_rows)
                    total_rows += len(batch)
                    batch.clear()
                    last_flush = now

        finally:
            if batch:
                self._flush_batch(conn, consumer, batch, total_rows)
                total_rows += len(batch)
            consumer.close()
            conn.close()
            skip_msg = f" | Skipped (board filter): {skipped}" if self._allowed_boards else ""
            print(f"[DONE] Inserted: {total_rows} rows{skip_msg}")

    # ── Internal ──────────────────────────────────────────────────

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

    def _create_db_conn(self):
        return psycopg2.connect(
            host=os.getenv("postgres_host", "localhost"),
            port=os.getenv("postgres_port", "5432"),
            dbname=os.getenv("postgres_db", "market_data"),
            user=os.getenv("postgres_user", "marketpulse"),
            password=os.getenv("postgres_password", "mp_secret_2026"),
        )

    def _flush_batch(self, conn, consumer, batch: list, total_rows: int) -> None:
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur, self._insert_sql, batch,
                    template=None,
                    page_size=len(batch),
                )
            conn.commit()
            consumer.commit()
            print(f"[FLUSH] +{len(batch)} rows | total: {total_rows + len(batch)}")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Flush failed: {e} — batch discarded, offset NOT committed")
