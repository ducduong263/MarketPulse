"""
archivers/quote_raw_archiver.py

Archives market.orderbook-l2 Kafka topic to Delta Lake (MinIO bronze layer).
"""
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
from confluent_kafka import Message
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingestion import DeltaLakeArchiver
from ingestion.common.avro_utils import unwrap_union, to_ts

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_TOPIC     = "market.orderbook-l2"
CONSUMER_GROUP  = "delta-quote-archiver-test-v1"
DELTA_TABLE_URI = "s3://market-data/bronze/market_quote"

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "order_book_l2.avsc"

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
    ("bid_levels", pa.list_(pa.struct([
        pa.field("price", pa.float64()),
        pa.field("qtty",  pa.int32()),
    ]))),
    ("ask_levels", pa.list_(pa.struct([
        pa.field("price", pa.float64()),
        pa.field("qtty",  pa.int32()),
    ]))),
    ("exchange_ts",   pa.timestamp("ms", tz="UTC")),
    ("dnse_ts",       pa.timestamp("ms", tz="UTC")),
    ("producer_ts",   pa.timestamp("ms", tz="UTC")),
    ("date",            pa.string()),
    ("kafka_partition", pa.int32()),
    ("kafka_offset",    pa.int64()),
])


# ── Record mapping ────────────────────────────────────────────────
def _record_to_row(record: dict, msg: Message) -> dict:
    exchange_ts = to_ts(record["exchange_ts"])
    date_str = exchange_ts.strftime("%Y-%m-%d") if exchange_ts else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return {
        "symbol":          record["symbol"],
        "market_id":       record["market_id"],
        "board_id":        record.get("board_id", ""),
        "bid_price1":      unwrap_union(record.get("bid_price1")),
        "bid_qty1":        unwrap_union(record.get("bid_qty1")),
        "bid_price2":      unwrap_union(record.get("bid_price2")),
        "bid_qty2":        unwrap_union(record.get("bid_qty2")),
        "bid_price3":      unwrap_union(record.get("bid_price3")),
        "bid_qty3":        unwrap_union(record.get("bid_qty3")),
        "ask_price1":      unwrap_union(record.get("ask_price1")),
        "ask_qty1":        unwrap_union(record.get("ask_qty1")),
        "ask_price2":      unwrap_union(record.get("ask_price2")),
        "ask_qty2":        unwrap_union(record.get("ask_qty2")),
        "ask_price3":      unwrap_union(record.get("ask_price3")),
        "ask_qty3":        unwrap_union(record.get("ask_qty3")),
        "total_bid_qty":   unwrap_union(record.get("total_bid_qty")),
        "total_ask_qty":   unwrap_union(record.get("total_ask_qty")),
        "bid_levels":      record.get("bid_levels") or [],
        "ask_levels":      record.get("ask_levels") or [],
        "exchange_ts":     exchange_ts,
        "dnse_ts":         to_ts(unwrap_union(record.get("dnse_ts"))),
        "producer_ts":     to_ts(unwrap_union(record.get("producer_ts"))),
        "date":            date_str,
        "kafka_partition": msg.partition(),
        "kafka_offset":    msg.offset(),
    }


# ── Entry point ───────────────────────────────────────────────────
if __name__ == "__main__":
    DeltaLakeArchiver(
        topic=KAFKA_TOPIC,
        consumer_group=CONSUMER_GROUP,
        schema_path=SCHEMA_PATH,
        delta_table_uri=DELTA_TABLE_URI,
        arrow_schema=ARROW_SCHEMA,
        record_to_row_fn=_record_to_row,
    ).run()
