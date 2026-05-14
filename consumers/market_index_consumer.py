import os
import time
import signal
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP     = os.getenv("kafka_bootstrap_servers", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("schema_registry_url", "http://localhost:8081")
KAFKA_TOPIC         = "market.index"
CONSUMER_GROUP      = "timescaledb-market-index-writer-v1"

DB_HOST     = os.getenv("postgres_host", "localhost")
DB_PORT     = os.getenv("postgres_port", "5432")
DB_NAME     = os.getenv("postgres_db", "market_data")
DB_USER     = os.getenv("postgres_user", "marketpulse")
DB_PASSWORD = os.getenv("postgres_password", "mp_secret_2026")

BATCH_SIZE    = 50
BATCH_TIMEOUT = 2.0

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "market_index.avsc"

# ── SQL ───────────────────────────────────────────────────────────
INSERT_SQL = """
INSERT INTO market_index (
    index_name, market_id,
    value, prior_value, highest_value, lowest_value, changed_value, changed_ratio,
    up_count, down_count, steady_count, upper_limit_count, lower_limit_count,
    up_volume, down_volume, steady_volume,
    total_volume, total_value, match_volume, match_value, deal_volume, deal_value,
    trading_session_id, exchange_ts, dnse_ts, producer_ts
) VALUES %s
"""

# ── Helpers ───────────────────────────────────────────────────────
def _unwrap_union(value):
    if value is None:
        return None
    if isinstance(value, dict):
        return next(iter(value.values()))
    return value


def _ms_to_ts(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _record_to_row(record: dict) -> tuple:
    def _u(k): return _unwrap_union(record.get(k))
    return (
        record["index_name"],
        record.get("market_id"),
        _u("value"), _u("prior_value"), _u("highest_value"), _u("lowest_value"),
        _u("changed_value"), _u("changed_ratio"),
        _u("up_count"), _u("down_count"), _u("steady_count"),
        _u("upper_limit_count"), _u("lower_limit_count"),
        _u("up_volume"), _u("down_volume"), _u("steady_volume"),
        _u("total_volume"), _u("total_value"),
        _u("match_volume"), _u("match_value"),
        _u("deal_volume"), _u("deal_value"),
        _u("trading_session_id"),
        _ms_to_ts(record["exchange_ts"]),
        _ms_to_ts(_u("dnse_ts")),
        _ms_to_ts(_u("producer_ts")),
    )


# ── Consumer & DB setup ───────────────────────────────────────────
def _create_consumer():
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        schema_str = f.read()
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(sr_client, schema_str)
    consumer = DeserializingConsumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           CONSUMER_GROUP,
        "auto.offset.reset":  "earliest",
        "value.deserializer": avro_deserializer,
        "enable.auto.commit": False,
    })
    return consumer


def _create_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    )


# ── Flush ─────────────────────────────────────────────────────────
def _flush_batch(conn, consumer, batch: list, total_rows: int):
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, INSERT_SQL, batch, page_size=len(batch))
        conn.commit()
        consumer.commit()
        print(f"[FLUSH] +{len(batch)} rows | total: {total_rows + len(batch)}")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Flush failed: {e} - offset NOT committed")


# ── Main ──────────────────────────────────────────────────────────
def run():
    consumer = _create_consumer()
    conn     = _create_db_conn()
    consumer.subscribe([KAFKA_TOPIC])

    batch      = []
    last_flush = time.monotonic()
    total_rows = 0

    print(f"[START] Consumer group: {CONSUMER_GROUP}")
    print(f"[CONFIG] Topic: {KAFKA_TOPIC} | Batch: {BATCH_SIZE} | Timeout: {BATCH_TIMEOUT}s")

    running = True

    def _signal_handler(sig, frame):
        nonlocal running
        print("\n[STOP] Shutting down consumer...")
        running = False

    signal.signal(signal.SIGINT,  _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        while running:
            msg = consumer.poll(timeout=0.5)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[ERROR] Kafka error: {msg.error()}")
            else:
                record = msg.value()
                if record is not None:
                    batch.append(_record_to_row(record))

            now = time.monotonic()
            if len(batch) >= BATCH_SIZE or (len(batch) > 0 and now - last_flush >= BATCH_TIMEOUT):
                _flush_batch(conn, consumer, batch, total_rows)
                total_rows += len(batch)
                batch.clear()
                last_flush = now

    finally:
        if batch:
            _flush_batch(conn, consumer, batch, total_rows)
            total_rows += len(batch)
        consumer.close()
        conn.close()
        print(f"[DONE] Inserted: {total_rows} rows")


if __name__ == "__main__":
    run()
