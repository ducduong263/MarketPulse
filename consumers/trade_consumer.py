import os
import sys
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
KAFKA_TOPIC         = "market.trade"
CONSUMER_GROUP      = "timescaledb-trade-writer-v1"

# Database
DB_HOST     = os.getenv("postgres_host", "localhost")
DB_PORT     = os.getenv("postgres_port", "5432")
DB_NAME     = os.getenv("postgres_db", "market_data")
DB_USER     = os.getenv("postgres_user", "marketpulse")
DB_PASSWORD = os.getenv("postgres_password", "mp_secret_2026")

# Batch tuning
BATCH_SIZE    = 100
BATCH_TIMEOUT = 2.0

ALLOWED_BOARDS = {"G1"}

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "market_trade.avsc"

# ── SQL ───────────────────────────────────────────────────────────
INSERT_SQL = """
INSERT INTO market_trade (
    symbol, market_id, board_id,
    price, quantity, side,
    session_vol, session_high, session_low, session_open, session_vwap,
    event_ts, received_ts
) VALUES %s
"""

# ── Helpers ───────────────────────────────────────────────────────
def _unwrap_union(value):
    """
    Avro union ["null", "type"] được deserialize thành {"type": value} hoặc None.
    Hàm này unwrap về giá trị thực.

    Ví dụ: {"long": 29} → 29 | {"double": 192.975} → 192.975 | None → None
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return next(iter(value.values()))
    return value


def _ms_to_ts(value):
    """Avro timestamp-millis → datetime UTC.
    AvroDeserializer có thể tự chuyển thành datetime, hoặc giữ nguyên int ms.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _record_to_row(record: dict) -> tuple:
    """Chuyển Avro record dict → tuple để insert vào TimescaleDB."""
    return (
        record["symbol"],
        record["market_id"],
        record.get("board_id", ""),
        record["price"],
        record["quantity"],
        record["side"],
        _unwrap_union(record.get("session_vol")),
        _unwrap_union(record.get("session_high")),
        _unwrap_union(record.get("session_low")),
        _unwrap_union(record.get("session_open")),
        _unwrap_union(record.get("session_vwap")),
        _ms_to_ts(record["event_ts"]),
        _ms_to_ts(_unwrap_union(record.get("received_ts"))),
    )


# ── Consumer setup ────────────────────────────────────────────────
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
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


# ── Flush ─────────────────────────────────────────────────────────
def _flush_batch(conn, consumer, batch: list, total_rows: int):
    """Insert batch vào TimescaleDB và commit Kafka offset."""
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur, INSERT_SQL, batch,
                template=None,
                page_size=len(batch),
            )
        conn.commit()
        consumer.commit()  # chỉ commit Kafka sau khi DB commit thành công
        print(f"[FLUSH] +{len(batch)} rows (G1 only) | total: {total_rows + len(batch)}")

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Flush failed: {e} — batch discarded, offset NOT committed")


# ── Main ──────────────────────────────────────────────────────────
def run():
    consumer = _create_consumer()
    conn     = _create_db_conn()
    consumer.subscribe([KAFKA_TOPIC])

    batch        = []
    last_flush   = time.monotonic()
    total_rows   = 0
    skipped      = 0

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
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    pass
                else:
                    print(f"[ERROR] Kafka error: {msg.error()}")
            else:
                record = msg.value()
                if record is not None:
                    board = record.get("board_id", "")
                    if board in ALLOWED_BOARDS:
                        batch.append(_record_to_row(record))
                    else:
                        skipped += 1

            # Flush batch khi đủ size hoặc timeout
            now = time.monotonic()
            should_flush = (
                len(batch) >= BATCH_SIZE or
                (len(batch) > 0 and now - last_flush >= BATCH_TIMEOUT)
            )

            if should_flush:
                _flush_batch(conn, consumer, batch, total_rows)
                total_rows += len(batch)
                batch.clear()
                last_flush = now

    finally:
        # Flush phần còn lại trước khi tắt
        if batch:
            _flush_batch(conn, consumer, batch, total_rows)
            total_rows += len(batch)
        consumer.close()
        conn.close()
        print(f"[DONE] Inserted: {total_rows} rows | Skipped (non-G1): {skipped}")


if __name__ == "__main__":
    run()
