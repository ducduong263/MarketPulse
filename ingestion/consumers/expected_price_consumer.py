"""
consumers/expected_price_consumer.py

Consumes market.expected-price topic and writes to TimescaleDB expected_price table.
"""
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingestion import KafkaTimescaleConsumer
from ingestion.common.avro_utils import unwrap_union, ms_to_ts

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_TOPIC    = "market.expected-price"
CONSUMER_GROUP = "timescaledb-expected-price-writer-v1"

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "expected_price.avsc"

# ── SQL ───────────────────────────────────────────────────────────
INSERT_SQL = """
INSERT INTO expected_price (
    symbol, market_id, board_id, isin,
    close_price, expected_price, expected_qty,
    producer_ts
) VALUES %s
"""


# ── Record mapping ────────────────────────────────────────────────
def _record_to_row(record: dict) -> tuple:
    def _u(k): return unwrap_union(record.get(k))
    return (
        record["symbol"],
        record.get("market_id"),
        record.get("board_id"),
        record.get("isin"),
        _u("close_price"),
        _u("expected_price"),
        _u("expected_qty"),
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
        batch_timeout=3.0,
            service_name="c-expected-price",
    ).run()
