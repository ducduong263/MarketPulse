"""
consumers/foreign_investor_consumer.py

Consumes market.foreign-investor topic and writes to TimescaleDB foreign_investor table.
"""
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingestion import KafkaTimescaleConsumer
from ingestion.common.avro_utils import unwrap_union, ms_to_ts

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_TOPIC    = "market.foreign-investor"
CONSUMER_GROUP = "timescaledb-foreign-investor-writer-v1"

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "foreign_investor.avsc"

# ── SQL ───────────────────────────────────────────────────────────
INSERT_SQL = """
INSERT INTO foreign_investor (
    symbol, market_id, board_id, trading_session_id,
    sell_volume, sell_value, buy_volume, buy_value,
    total_sell_volume, total_sell_value,
    total_buy_volume, total_buy_value,
    order_limit_qty, buy_possible_qty,
    exchange_ts, producer_ts
) VALUES %s
"""


# ── Record mapping ────────────────────────────────────────────────
def _record_to_row(record: dict) -> tuple:
    def _u(k): return unwrap_union(record.get(k))
    return (
        record["symbol"],
        record.get("market_id"),
        record.get("board_id"),
        record.get("trading_session_id"),
        _u("sell_volume"),
        _u("sell_value"),
        _u("buy_volume"),
        _u("buy_value"),
        _u("total_sell_volume"),
        _u("total_sell_value"),
        _u("total_buy_volume"),
        _u("total_buy_value"),
        _u("order_limit_qty"),
        _u("buy_possible_qty"),
        ms_to_ts(_u("exchange_ts")),
        ms_to_ts(record["producer_ts"]),
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
    ).run()
