"""
archivers/trade_raw_archiver.py

Archives market.trade Kafka topic to Delta Lake (MinIO bronze layer).
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
KAFKA_TOPIC     = "market.trade"
CONSUMER_GROUP  = "delta-trade-archiver-v1"
DELTA_TABLE_URI = "s3://market-data/bronze/market_trade"

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "market_trade.avsc"

# ── Arrow Schema ──────────────────────────────────────────────────
ARROW_SCHEMA = pa.schema([
    ("symbol",          pa.string()),
    ("market_id",       pa.string()),
    ("board_id",        pa.string()),
    ("price",           pa.float64()),
    ("quantity",        pa.int32()),
    ("side",            pa.int32()),
    ("session_vol",     pa.int64()),
    ("session_high",    pa.float64()),
    ("session_low",     pa.float64()),
    ("session_open",    pa.float64()),
    ("session_vwap",    pa.float64()),
    ("trading_session_id", pa.string()),
    ("exchange_ts",     pa.timestamp("ms", tz="UTC")),
    ("dnse_ts",         pa.timestamp("ms", tz="UTC")),
    ("producer_ts",     pa.timestamp("ms", tz="UTC")),
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
        "price":           record["price"],
        "quantity":        record["quantity"],
        "side":            record["side"],
        "session_vol":     unwrap_union(record.get("session_vol")),
        "session_high":    unwrap_union(record.get("session_high")),
        "session_low":     unwrap_union(record.get("session_low")),
        "session_open":    unwrap_union(record.get("session_open")),
        "session_vwap":    unwrap_union(record.get("session_vwap")),
        "trading_session_id": unwrap_union(record.get("trading_session_id")),
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