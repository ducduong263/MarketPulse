"""
consumers/quote_consumer.py

Consumes market.orderbook-l2 topic and writes to TimescaleDB order_book_l2 table.
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
KAFKA_TOPIC    = "market.orderbook-l2"
CONSUMER_GROUP = "timescaledb-quote-writer-v1"
ALLOWED_BOARDS = {"G1"}

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "order_book_l2.avsc"

# ── SQL ───────────────────────────────────────────────────────────
INSERT_SQL = """
INSERT INTO order_book_l2 (
    symbol, market_id,
    bid_price1, bid_qty1,
    bid_price2, bid_qty2,
    bid_price3, bid_qty3,
    ask_price1, ask_qty1,
    ask_price2, ask_qty2,
    ask_price3, ask_qty3,
    exchange_ts, dnse_ts, producer_ts
) VALUES %s
"""


# ── Record mapping ────────────────────────────────────────────────
def _record_to_row(record: dict) -> tuple:
    return (
        record["symbol"],
        record["market_id"],
        unwrap_union(record.get("bid_price1")),
        unwrap_union(record.get("bid_qty1")),
        unwrap_union(record.get("bid_price2")),
        unwrap_union(record.get("bid_qty2")),
        unwrap_union(record.get("bid_price3")),
        unwrap_union(record.get("bid_qty3")),
        unwrap_union(record.get("ask_price1")),
        unwrap_union(record.get("ask_qty1")),
        unwrap_union(record.get("ask_price2")),
        unwrap_union(record.get("ask_qty2")),
        unwrap_union(record.get("ask_price3")),
        unwrap_union(record.get("ask_qty3")),
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
        batch_timeout=0.5,
        allowed_boards=ALLOWED_BOARDS,
        service_name="c-quote",
    ).run()

