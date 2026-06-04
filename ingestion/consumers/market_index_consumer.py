"""
consumers/market_index_consumer.py

Consumes market.index topic and writes to TimescaleDB market_index table.
"""
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingestion import KafkaTimescaleConsumer
from ingestion.common.avro_utils import unwrap_union, ms_to_ts

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_TOPIC    = "market.index"
CONSUMER_GROUP = "timescaledb-market-index-writer-v1"

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "market_index.avsc"

# ── SQL ───────────────────────────────────────────────────────────
INSERT_SQL = """
INSERT INTO market_index (
    index_name, market_id,
    value, prior_value, highest_value, lowest_value, changed_value, changed_ratio,
    up_count, down_count, steady_count, upper_limit_count, lower_limit_count,
    up_volume, down_volume, steady_volume,
    total_volume, total_value, match_volume, match_value, deal_volume, deal_value,
    trading_session_id, exchange_ts, dnse_ts, producer_ts
) VALUES %s
"""


# ── Record mapping ────────────────────────────────────────────────
def _record_to_row(record: dict) -> tuple:
    def _u(k): return unwrap_union(record.get(k))
    return (
        record["index_name"],
        record.get("market_id"),
        _u("value"), _u("prior_value"), _u("highest_value"), _u("lowest_value"),
        _u("changed_value"), _u("changed_ratio"),
        _u("up_count"), _u("down_count"), _u("steady_count"),
        _u("upper_limit_count"), _u("lower_limit_count"),
        _u("up_volume"), _u("down_volume"), _u("steady_volume"),
        _u("total_volume"), _u("total_value"),
        _u("match_volume"), _u("match_value"),
        _u("deal_volume"), _u("deal_value"),
        _u("trading_session_id"),
        ms_to_ts(record["exchange_ts"]),
        ms_to_ts(_u("dnse_ts")),
        ms_to_ts(_u("producer_ts")),
    )


# ── Entry point ───────────────────────────────────────────────────
if __name__ == "__main__":
    KafkaTimescaleConsumer(
        topic=KAFKA_TOPIC,
        consumer_group=CONSUMER_GROUP,
        schema_path=SCHEMA_PATH,
        insert_sql=INSERT_SQL,
        record_to_row_fn=_record_to_row,
        batch_size=50,
        batch_timeout=2.0,
    ).run()
