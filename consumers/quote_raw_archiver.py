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
KAFKA_TOPIC         = "market.orderbook-l2"
CONSUMER_GROUP      = "delta-quote-archiver-test-v1"

# Delta Lake / MinIO
MINIO_ENDPOINT  = os.getenv("minio_endpoint", "localhost:9000")
MINIO_ACCESS    = os.getenv("minio_root_user", "minioadmin")
MINIO_SECRET    = os.getenv("minio_root_password", "minioadmin")
DELTA_TABLE_URI = "s3://market-data/bronze/market_quote"

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL":           f"http://{MINIO_ENDPOINT}",
    "AWS_ACCESS_KEY_ID":          MINIO_ACCESS,
    "AWS_SECRET_ACCESS_KEY":      MINIO_SECRET,
    "AWS_REGION":                 "us-east-1",
    "AWS_ALLOW_HTTP":             "true",
    "AWS_FORCE_PATH_STYLE":      "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

FLUSH_INTERVAL = 300
FLUSH_SIZE     = 5000
MAX_RETRY      = 3

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "order_book_l2.avsc"

MINIO_BUCKET = "market-data"


# ── Bucket setup ─────────────────────────────────────────────────
def _ensure_bucket():
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
    ("symbol",        pa.string()),
    ("market_id",     pa.string()),
    ("board_id",      pa.string()),
    ("bid_price1",    pa.float64()),
    ("bid_qty1",      pa.int32()),
    ("bid_price2",    pa.float64()),
    ("bid_qty2",      pa.int32()),
    ("bid_price3",    pa.float64()),
    ("bid_qty3",      pa.int32()),
    ("ask_price1",    pa.float64()),
    ("ask_qty1",      pa.int32()),
    ("ask_price2",    pa.float64()),
    ("ask_qty2",      pa.int32()),
    ("ask_price3",    pa.float64()),
    ("ask_qty3",      pa.int32()),
    ("total_bid_qty", pa.int64()),
    ("total_ask_qty", pa.int64()),
    
    ("bid_levels",    pa.list_(pa.struct([
        pa.field("price", pa.float64()),
        pa.field("qtty",  pa.int32()),
    ]))),
    ("ask_levels",    pa.list_(pa.struct([
        pa.field("price", pa.float64()),
        pa.field("qtty",  pa.int32()),
    ]))),
    ("exchange_ts",   pa.timestamp("ms", tz="UTC")),  # exchange_ts = sending_time
    ("dnse_ts",       pa.timestamp("ms", tz="UTC")),  # dnse_ts = multicast_receive_time
    ("producer_ts",   pa.timestamp("ms", tz="UTC")),  # producer_ts = _receivedAt
    
    ("date",            pa.string()),   # partition key  "YYYY-MM-DD"
    ("kafka_partition", pa.int32()),    # dedup key part 1
    ("kafka_offset",    pa.int64()),    # dedup key part 2
])


# ── Helpers ───────────────────────────────────────────────────────
def _unwrap_union(value):
    if value is None:
        return None
    if isinstance(value, dict):
        return next(iter(value.values()))
    return value

def _to_ts(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _record_to_row(record: dict, msg: Message) -> dict:
    exchange_ts = _to_ts(record["exchange_ts"])
    date_str  = exchange_ts.strftime("%Y-%m-%d") if exchange_ts else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return {
        "symbol":        record["symbol"],
        "market_id":     record["market_id"],
        "board_id":      record.get("board_id", ""),
        "bid_price1":    _unwrap_union(record.get("bid_price1")),
        "bid_qty1":      _unwrap_union(record.get("bid_qty1")),
        "bid_price2":    _unwrap_union(record.get("bid_price2")),
        "bid_qty2":      _unwrap_union(record.get("bid_qty2")),
        "bid_price3":    _unwrap_union(record.get("bid_price3")),
        "bid_qty3":      _unwrap_union(record.get("bid_qty3")),
        "ask_price1":    _unwrap_union(record.get("ask_price1")),
        "ask_qty1":      _unwrap_union(record.get("ask_qty1")),
        "ask_price2":    _unwrap_union(record.get("ask_price2")),
        "ask_qty2":      _unwrap_union(record.get("ask_qty2")),
        "ask_price3":    _unwrap_union(record.get("ask_price3")),
        "ask_qty3":      _unwrap_union(record.get("ask_qty3")),
        "total_bid_qty": _unwrap_union(record.get("total_bid_qty")),
        "total_ask_qty": _unwrap_union(record.get("total_ask_qty")),
        
        "bid_levels":    record.get("bid_levels") or [],
        "ask_levels":    record.get("ask_levels") or [],
        "exchange_ts":   exchange_ts,
        "dnse_ts":       _to_ts(_unwrap_union(record.get("dnse_ts"))),
        "producer_ts":   _to_ts(_unwrap_union(record.get("producer_ts"))),

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
    if not buffer:
        return 0

    table = pa.Table.from_pylist(buffer, schema=ARROW_SCHEMA)

    write_deltalake(
        DELTA_TABLE_URI,
        table,
        mode="append",
        partition_by=["date"],
        storage_options=STORAGE_OPTIONS,
        schema_mode="merge",
    )

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
    last_buffered_msg = None
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
                    print(f"[ERROR] Flush failed after {MAX_RETRY} attempts — offset NOT committed")
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
