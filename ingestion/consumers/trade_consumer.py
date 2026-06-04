"""
consumers/trade_consumer.py

Consumes market.trade topic and writes to TimescaleDB market_trade table.
Filters to G1 (continuous) board only.
"""
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingestion import KafkaTimescaleConsumer
from ingestion.common.avro_utils import unwrap_union, ms_to_ts

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_TOPIC    = "market.trade"
CONSUMER_GROUP = "timescaledb-trade-writer-v1"
ALLOWED_BOARDS = {"G1"}

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "market_trade.avsc"

# ── SQL ───────────────────────────────────────────────────────────
INSERT_SQL = """
INSERT INTO market_trade (
    symbol, market_id, board_id,
    price, quantity, side,
    session_vol, session_high, session_low, session_open, session_vwap,
    exchange_ts, dnse_ts, producer_ts
) VALUES %s
"""


# ── Record mapping ────────────────────────────────────────────────
def _record_to_row(record: dict) -> tuple:
    return (
        record["symbol"],
        record["market_id"],
        record.get("board_id", ""),
        record["price"],
        record["quantity"],
        record["side"],
        unwrap_union(record.get("session_vol")),
        unwrap_union(record.get("session_high")),
        unwrap_union(record.get("session_low")),
        unwrap_union(record.get("session_open")),
        unwrap_union(record.get("session_vwap")),
        ms_to_ts(record["exchange_ts"]),
        ms_to_ts(unwrap_union(record.get("dnse_ts"))),
        ms_to_ts(unwrap_union(record.get("producer_ts"))),
    )


# ── Entry point ───────────────────────────────────────────────────
if __name__ == "__main__":
    KafkaTimescaleConsumer(
        topic=KAFKA_TOPIC,
        consumer_group=CONSUMER_GROUP,
        schema_path=SCHEMA_PATH,
        insert_sql=INSERT_SQL,
        record_to_row_fn=_record_to_row,
        batch_size=100,
        batch_timeout=2.0,
        allowed_boards=ALLOWED_BOARDS,
    ).run()
