"""
ingestion/common/consumer_base.py

Base class for all Kafka Avro -> TimescaleDB consumers.

Handles:
- Kafka DeserializingConsumer setup (Avro + Schema Registry)
- PostgreSQL connection via psycopg2
- Batch insert with execute_values
- Graceful shutdown (SIGINT/SIGTERM)
- Optional board_id filtering (ALLOWED_BOARDS)
- StatsReporter: periodic flush of health metrics to pipeline_stats table
- Fetch tuning: fetch.min.bytes / fetch.wait.max.ms configurable per consumer
"""
from __future__ import annotations

import os
import signal
import time
from pathlib import Path
from typing import Callable

import psycopg2
import psycopg2.extras
from confluent_kafka import DeserializingConsumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from .avro_utils import unwrap_union, ms_to_ts  # noqa: F401 — re-exported for consumers
from .stats_reporter import StatsReporter


class KafkaTimescaleConsumer:
    """
    Reusable Kafka Avro -> TimescaleDB batch consumer.

    Each concrete consumer provides:
      - topic:              Kafka topic name
      - consumer_group:     Consumer group ID
      - schema_path:        Path to .avsc schema file
      - insert_sql:         SQL with VALUES %s placeholder
      - record_to_row_fn:   Function (record: dict) -> tuple for DB insert
      - batch_size:         Flush when batch reaches this size (default 100)
      - batch_timeout:      Flush when batch age exceeds this (seconds, default 2.0)
      - allowed_boards:     Optional set of board_id strings to allow (None = no filter)
      - fetch_min_bytes:    kafka fetch.min.bytes — wait until broker has at least N bytes
                            before sending a fetch response. Higher = fewer round-trips,
                            more latency. Default 1 (confluent-kafka default, no batching).
                            Recommended 8192 for high-volume topics (e.g. quote, archive).
      - fetch_wait_max_ms:  kafka fetch.wait.max.ms — max time broker waits to fill
                            fetch_min_bytes. Default 500ms. Lower = more responsive,
                            higher = better CPU efficiency.
                            Recommended 100ms paired with fetch_min_bytes=8192.

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
            # High-volume topic tuning:
            fetch_min_bytes=8192,
            fetch_wait_max_ms=100,
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
        service_name: str | None = None,
        fetch_min_bytes: int = 1,
        fetch_wait_max_ms: int = 500,
    ) -> None:
        self.topic = topic
        self.consumer_group = consumer_group
        self._schema_path = schema_path
        self._insert_sql = insert_sql
        self._record_to_row_fn = record_to_row_fn
        self._batch_size = batch_size
        self._batch_timeout = batch_timeout
        self._allowed_boards = allowed_boards  # None means accept all boards
        self._fetch_min_bytes = fetch_min_bytes
        self._fetch_wait_max_ms = fetch_wait_max_ms

        # ── Stats reporter ────────────────────────────────────────────────────
        _svc = service_name or self.__class__.__name__.lower()
        self._stats = StatsReporter(service_name=_svc)

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
        consumer = None
        conn = None
        try:
            consumer = self._create_consumer()
            conn = self._create_db_conn()
            consumer.subscribe([self.topic])
        except Exception as e:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            raise e

        batch: list[tuple] = []
        last_flush = time.monotonic()
        last_lag_check = time.monotonic()
        last_skip_commit = time.monotonic()   # periodic commit for skipped msgs
        total_rows = 0
        skipped = 0
        skipped_since_commit = 0              # skipped since last explicit commit

        print(f"[START] Consumer group: {self.consumer_group}")
        print(f"[CONFIG] Topic: {self.topic} | Batch: {self._batch_size} | Timeout: {self._batch_timeout}s")
        print(f"[CONFIG] Fetch: min_bytes={self._fetch_min_bytes} wait_max_ms={self._fetch_wait_max_ms}")
        if self._allowed_boards:
            print(f"[CONFIG] Board filter: {self._allowed_boards}")

        self._stats.start()
        self._stats.mark_online()  # consumer is running

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
                                    skipped_since_commit += 1
                                    continue
                            batch.append(self._record_to_row_fn(record))
                            self._stats.inc_msg()

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
                    last_skip_commit = now     # _flush_batch already calls commit()
                    skipped_since_commit = 0

                # ── Periodic commit for skipped (filtered) messages ──────────
                # Ensures non-G1 messages don't accumulate as permanent
                # consumer lag when the batch never fills (e.g. end of day).
                # asynchronous=True: safe here since no DB write is involved —
                # worst case on failure we re-filter the same messages on restart.
                elif skipped_since_commit > 0 and now - last_skip_commit >= 5:
                    try:
                        consumer.commit(asynchronous=True)
                        last_skip_commit = now
                        skipped_since_commit = 0
                    except Exception:
                        pass  # best-effort, not critical

                # ── Consumer lag check every 10s ───────────────────
                if now - last_lag_check >= 10:
                    self._update_lag(consumer)
                    last_lag_check = now

        finally:
            if batch and conn is not None and consumer is not None:
                self._flush_batch(conn, consumer, batch, total_rows)
                total_rows += len(batch)
            self._stats.mark_offline()  # consumer shutting down
            self._stats.stop()  # final metrics flush
            if consumer is not None:
                consumer.close()
            if conn is not None:
                conn.close()
            skip_msg = f" | Skipped (board filter): {skipped}" if self._allowed_boards else ""
            print(f"[DONE] Inserted: {total_rows} rows{skip_msg}")

    # ── Internal ──────────────────────────────────────────────────

    def _update_lag(self, consumer: DeserializingConsumer) -> None:
        """Query current consumer lag from Kafka and report to StatsReporter."""
        try:
            partitions = consumer.assignment()
            if not partitions:
                return
            total_lag = 0
            for tp in partitions:
                lo, hi = consumer.get_watermark_offsets(tp, timeout=0.5, cached=True)
                committed = consumer.committed([tp], timeout=0.5)
                current = committed[0].offset if committed and committed[0].offset >= 0 else lo
                total_lag += max(0, hi - current)
            self._stats.set_consumer_lag(float(total_lag), self.topic)
        except Exception:
            pass  # lag check is best-effort

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
            # ── Fetch tuning ────────────────────────────────────────────────────
            # fetch.min.bytes: broker waits until it has this many bytes before
            # sending a response. Higher value = fewer round-trips, lower CPU.
            # fetch.wait.max.ms: max broker wait time to satisfy fetch.min.bytes.
            # Together they act as a server-side batching knob.
            "fetch.min.bytes":    self._fetch_min_bytes,
            "fetch.wait.max.ms":  self._fetch_wait_max_ms,
        })

    def _create_db_conn(self):
        retries = 10
        delay = 3
        for i in range(retries):
            try:
                return psycopg2.connect(
                    host=os.getenv("postgres_host", "localhost"),
                    port=os.getenv("postgres_port", "5432"),
                    dbname=os.getenv("postgres_db", "market_data"),
                    user=os.getenv("postgres_user", "marketpulse"),
                    password=os.getenv("postgres_password", "mp_secret_2026"),
                )
            except psycopg2.OperationalError as e:
                if i < retries - 1:
                    print(f"[DB_CONN] Database system starting or connection failed: {e}. Retrying in {delay}s... ({i+1}/{retries})")
                    time.sleep(delay)
                else:
                    raise e

    def _flush_batch(self, conn, consumer, batch: list, total_rows: int) -> None:
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur, self._insert_sql, batch,
                    template=None,
                    page_size=len(batch),
                )
            conn.commit()
            consumer.commit(asynchronous=True)
            print(f"[FLUSH] +{len(batch)} rows | total: {total_rows + len(batch)}")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Flush failed: {e} — batch discarded, offset NOT committed")
