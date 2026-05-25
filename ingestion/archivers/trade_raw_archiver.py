import os
import time
import signal
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake
import boto3
from botocore.exceptions import ClientError
from confluent_kafka import DeserializingConsumer, KafkaError, Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP     = os.getenv("kafka_bootstrap_servers", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("schema_registry_url", "http://localhost:8081")
KAFKA_TOPIC         = "market.trade"
CONSUMER_GROUP      = "delta-trade-archiver-v1"

# Delta Lake / MinIO
MINIO_ENDPOINT  = os.getenv("minio_endpoint", "localhost:9000")
MINIO_ACCESS    = os.getenv("minio_root_user", "minioadmin")
MINIO_SECRET    = os.getenv("minio_root_password", "minioadmin")
DELTA_TABLE_URI = "s3://market-data/bronze/market_trade"

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL":          f"http://{MINIO_ENDPOINT}",
    "AWS_ACCESS_KEY_ID":         MINIO_ACCESS,
    "AWS_SECRET_ACCESS_KEY":     MINIO_SECRET,
    "AWS_REGION":                "us-east-1",
    "AWS_ALLOW_HTTP":            "true",
}

FLUSH_INTERVAL = 300    # seconds
FLUSH_SIZE     = 5_000  # records
MAX_RETRY      = 3

SCHEMA_PATH = Path(__file__).resolve().parent.parent.parent / "schemas" / "market_trade.avsc"

MINIO_BUCKET = "market-data"


# ── Bucket setup ─────────────────────────────────────────────────
def _ensure_bucket():
    """Create MinIO bucket if it doesn't exist (delta-rs doesn't auto-create)."""
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1",
    )
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=MINIO_BUCKET)
        print(f"[MINIO] Created bucket: {MINIO_BUCKET}")

# ── Arrow Schema ──────────────────────────────────────────────────
ARROW_SCHEMA = pa.schema([
    # market data fields
    ("symbol",        pa.string()),
    ("market_id",     pa.string()),
    ("board_id",      pa.string()),
    ("price",         pa.float64()),
    ("quantity",      pa.int32()),
    ("side",          pa.int32()),
    ("session_vol",   pa.int64()),
    ("session_high",  pa.float64()),
    ("session_low",   pa.float64()),
    ("session_open",  pa.float64()),
    ("session_vwap",  pa.float64()),
    ("exchange_ts",   pa.timestamp("ms", tz="UTC")),  # exchange_ts = sending_time
    ("dnse_ts",       pa.timestamp("ms", tz="UTC")),  # dnse_ts = multicast_receive_time
    ("producer_ts",   pa.timestamp("ms", tz="UTC")),  # producer_ts = _receivedAt
    # lakehouse metadata
    ("date",            pa.string()),     # partition key  "YYYY-MM-DD"
    ("kafka_partition", pa.int32()),      # dedup key part 1
    ("kafka_offset",    pa.int64()),      # dedup key part 2
])


# ── Helpers ───────────────────────────────────────────────────────
def _unwrap_union(value):
    """Avro union {"type": value} → actual value."""
    if value is None:
        return None
    if isinstance(value, dict):
        return next(iter(value.values()))
    return value


def _to_ts(value):
    """Avro timestamp (datetime | int ms) → timezone-aware datetime UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _record_to_row(record: dict, msg: Message) -> dict:
    """Avro record + Kafka message metadata → dict for PyArrow."""
    exchange_ts = _to_ts(record["exchange_ts"])
    date_str = exchange_ts.strftime("%Y-%m-%d") if exchange_ts else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return {
        "symbol":       record["symbol"],
        "market_id":    record["market_id"],
        "board_id":     record.get("board_id", ""),
        "price":        record["price"],
        "quantity":     record["quantity"],
        "side":         record["side"],
        "session_vol":  _unwrap_union(record.get("session_vol")),
        "session_high": _unwrap_union(record.get("session_high")),
        "session_low":  _unwrap_union(record.get("session_low")),
        "session_open": _unwrap_union(record.get("session_open")),
        "session_vwap": _unwrap_union(record.get("session_vwap")),
        "exchange_ts":  exchange_ts,
        "dnse_ts":      _to_ts(_unwrap_union(record.get("dnse_ts"))),
        "producer_ts":  _to_ts(_unwrap_union(record.get("producer_ts"))),
        # lakehouse metadata
        "date":             date_str,
        "kafka_partition":  msg.partition(),
        "kafka_offset":     msg.offset(),
    }


# ── Consumer setup ────────────────────────────────────────────────
def _create_consumer() -> DeserializingConsumer:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        schema_str = f.read()

    sr_client         = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(sr_client, schema_str)

    return DeserializingConsumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           CONSUMER_GROUP,
        "auto.offset.reset":  "earliest",
        "value.deserializer": avro_deserializer,
        "enable.auto.commit": False,
    })


# ── Flush to Delta Lake ───────────────────────────────────────────
def _flush(consumer: DeserializingConsumer, buffer: list, last_msg: Message) -> int:
    """Write buffer → Delta Lake, then commit Kafka offset explicitly (at-least-once)."""
    if not buffer:
        return 0

    table = pa.Table.from_pylist(buffer, schema=ARROW_SCHEMA)

    # Write to Delta Lake FIRST, commit Kafka offset AFTER
    write_deltalake(
        DELTA_TABLE_URI,
        table,
        mode="append",
        partition_by=["date"],
        storage_options=STORAGE_OPTIONS,
        schema_mode="merge",
    )

    # Commit the exact offset of the last message in the buffer — explicit is safer
    # than consumer.commit() which would commit whatever the internal cursor is at
    consumer.commit(message=last_msg)

    n = len(buffer)
    print(f"[FLUSH] {n} records → {DELTA_TABLE_URI} (date={buffer[0]['date']}, offset={last_msg.offset()})") 
    return n


# ── Main ──────────────────────────────────────────────────────────
def run():
    _ensure_bucket()
    consumer = _create_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    buffer            = []
    last_buffered_msg = None  # track last message added to buffer for explicit commit
    last_flush        = time.monotonic()
    total             = 0
    running           = True

    def _stop(sig, frame):
        nonlocal running
        print("\n[STOP] Shutting down...")
        running = False

    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    print(f"[START] Consumer group: {CONSUMER_GROUP}")
    print(f"[CONFIG] {KAFKA_TOPIC} → Delta Lake {DELTA_TABLE_URI}")
    print(f"[CONFIG] Flush every {FLUSH_INTERVAL}s or {FLUSH_SIZE} records")

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[ERROR] Kafka: {msg.error()}")
            else:
                record = msg.value()
                if record is not None:
                    buffer.append(_record_to_row(record, msg))
                    last_buffered_msg = msg  # always track last message in buffer

            now = time.monotonic()
            should_flush = (
                len(buffer) >= FLUSH_SIZE or
                (len(buffer) > 0 and now - last_flush >= FLUSH_INTERVAL)
            )

            if should_flush and last_buffered_msg is not None:
                for attempt in range(MAX_RETRY):
                    try:
                        total += _flush(consumer, buffer, last_buffered_msg)
                        buffer.clear()
                        last_buffered_msg = None
                        last_flush = now
                        break
                    except Exception as e:
                        print(f"[WARN] Flush attempt {attempt + 1}/{MAX_RETRY}: {e}")
                        time.sleep(2 ** attempt)
                else:
                    # Exhausted retries — do NOT commit, let Kafka re-deliver
                    print(f"[ERROR] Flush failed after {MAX_RETRY} attempts — buffer cleared, offset NOT committed")
                    buffer.clear()
                    last_buffered_msg = None
                    last_flush = now

    finally:
        if buffer and last_buffered_msg is not None:
            try:
                total += _flush(consumer, buffer, last_buffered_msg)
            except Exception as e:
                print(f"[ERROR] Final flush failed: {e}")
        consumer.close()
        print(f"[DONE] Total archived: {total} records")


if __name__ == "__main__":
    run()