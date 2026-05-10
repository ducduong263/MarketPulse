# consumers/quote_raw_archiver.py
"""
Market Quote Raw Archiver (Cold Path)
─────────────────────────────────────
Kafka topic `market.orderbook-l2` → Parquet → MinIO.

Lưu trữ dữ liệu sổ lệnh thô vào MinIO dưới dạng Parquet,
phân vùng theo ngày để phục vụ batch analytics và ML.

Cấu trúc file trên MinIO:
  raw/market_quote/date=2026-04-20/103000_103500.parquet
"""

import os
import io
import time
import signal
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP     = os.getenv("kafka_bootstrap_servers", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("schema_registry_url", "http://localhost:8081")
KAFKA_TOPIC         = "market.orderbook-l2"
CONSUMER_GROUP      = "minio-quote-archiver-v1"

# MinIO
MINIO_ENDPOINT  = os.getenv("minio_endpoint", "localhost:9000")
MINIO_ACCESS    = os.getenv("minio_root_user", "minioadmin")
MINIO_SECRET    = os.getenv("minio_root_password", "minioadmin")
MINIO_BUCKET    = "market-data"
MINIO_PREFIX    = "raw/market_quote"

# Flush mỗi 5 phút hoặc 5000 records
FLUSH_INTERVAL  = 300   # giây (5 phút)
FLUSH_SIZE      = 5000  # records
MAX_RETRY       = 3     # số lần retry khi upload MinIO thất bại

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "order_book_l2.avsc"

# ── Arrow Schema ─────────────────────────────────────────────────
# Định nghĩa schema cố định cho Parquet file
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
    ("event_ts",      pa.timestamp("ms", tz="UTC")),
    ("received_ts",   pa.timestamp("ms", tz="UTC")),
])


# ── Helpers ───────────────────────────────────────────────────────
def _unwrap_union(value):
    """Avro union {"type": value} → giá trị thực."""
    if value is None:
        return None
    if isinstance(value, dict):
        return next(iter(value.values()))
    return value


def _to_ts(value):
    """Chuyển Avro timestamp (datetime hoặc int ms) → datetime UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _record_to_dict(record: dict) -> dict:
    """Avro record → dict chuẩn cho PyArrow."""
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
        "event_ts":      _to_ts(record["event_ts"]),
        "received_ts":   _to_ts(_unwrap_union(record.get("received_ts"))),
    }


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


def _create_minio():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=False,  # localhost không dùng HTTPS
    )
    # Tạo bucket nếu chưa có
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        print(f"[MINIO] Created bucket: {MINIO_BUCKET}")
    return client


# ── Flush to MinIO ────────────────────────────────────────────────
def _flush_to_minio(minio_client, consumer, buffer: list, flush_start: str):
    """Chuyển buffer → Parquet in-memory → upload MinIO."""
    if not buffer:
        return

    # Tạo PyArrow Table — from_pylist tự handle missing keys và cast type
    table = pa.Table.from_pylist(buffer, schema=ARROW_SCHEMA)

    # Xác định đường dẫn file trên MinIO — lấy date từ event_ts của record đầu
    first_event_ts = buffer[0]["event_ts"]
    date_str = first_event_ts.strftime("%Y-%m-%d")
    flush_end = datetime.now(timezone.utc).strftime("%H%M%S")
    object_name = f"{MINIO_PREFIX}/date={date_str}/{flush_start}_{flush_end}.parquet"

    # Ghi Parquet vào bộ nhớ
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    file_size = buf.getbuffer().nbytes

    # Upload lên MinIO
    minio_client.put_object(
        MINIO_BUCKET,
        object_name,
        buf,
        length=file_size,
        content_type="application/octet-stream",
    )

    # Commit Kafka offset sau khi upload thành công
    consumer.commit()

    size_kb = file_size / 1024
    print(f"[FLUSH] {len(buffer)} records → {object_name} ({size_kb:.1f} KB)")


# ── Main ──────────────────────────────────────────────────────────
def run():
    consumer     = _create_consumer()
    minio_client = _create_minio()
    consumer.subscribe([KAFKA_TOPIC])

    buffer       = []
    flush_start  = datetime.now(timezone.utc).strftime("%H%M%S")
    last_flush   = time.monotonic()
    total_records = 0

    print(f"[START] Consumer group: {CONSUMER_GROUP}")
    print(f"[CONFIG] Topic: {KAFKA_TOPIC} → MinIO s3://{MINIO_BUCKET}/{MINIO_PREFIX}/")
    print(f"[CONFIG] Flush: mỗi {FLUSH_INTERVAL}s hoặc {FLUSH_SIZE} records | Format: Parquet + Snappy")

    running = True

    def _signal_handler(sig, frame):
        nonlocal running
        print("\n[STOP] Shutting down archiver...")
        running = False

    signal.signal(signal.SIGINT,  _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    pass
                else:
                    print(f"[ERROR] Kafka: {msg.error()}")
            else:
                record = msg.value()
                if record is not None:
                    buffer.append(_record_to_dict(record))

            # Flush khi đủ size hoặc timeout
            now = time.monotonic()
            should_flush = (
                len(buffer) >= FLUSH_SIZE or
                (len(buffer) > 0 and now - last_flush >= FLUSH_INTERVAL)
            )

            if should_flush:
                for attempt in range(MAX_RETRY):
                    try:
                        _flush_to_minio(minio_client, consumer, buffer, flush_start)
                        total_records += len(buffer)
                        buffer.clear()
                        flush_start = datetime.now(timezone.utc).strftime("%H%M%S")
                        last_flush = now
                        break
                    except Exception as e:
                        print(f"[WARN] Upload attempt {attempt+1}/{MAX_RETRY} failed: {e}")
                        time.sleep(2 ** attempt)  # exponential backoff: 1s, 2s, 4s
                else:
                    # Fail sau MAX_RETRY lần → drop buffer, không commit offset
                    print(f"[ERROR] Upload failed after {MAX_RETRY} attempts — clearing buffer, offset NOT committed")
                    buffer.clear()
                    flush_start = datetime.now(timezone.utc).strftime("%H%M%S")
                    last_flush = now

    finally:
        # Flush phần còn lại
        if buffer:
            try:
                _flush_to_minio(minio_client, consumer, buffer, flush_start)
                total_records += len(buffer)
            except Exception as e:
                print(f"[ERROR] Final flush failed: {e}")
        consumer.close()
        print(f"[DONE] Total archived: {total_records} records")


if __name__ == "__main__":
    run()
