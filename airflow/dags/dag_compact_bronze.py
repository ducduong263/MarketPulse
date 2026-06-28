"""
dag_compact_bronze - Optimize Delta Lake bronze layer after each trading session.

Schedule: 16:00 ICT daily (Mon-Fri) = 09:00 UTC

Tasks:
  check_trading_day       : Skip if today is not a trading day.
  optimize_market_trade   : COMPACT + Z-ORDER market_trade for today's partition.
  optimize_market_quote   : COMPACT + Z-ORDER market_quote for today's partition.
  vacuum_market_trade     : (Fridays only) Delete tombstoned files older than 48h.
  vacuum_market_quote     : (Fridays only) Delete tombstoned files older than 48h.
  log_summary             : Log metrics from all tasks.

Expected: ~48 files/day/table -> 1-2 files after COMPACT.
VACUUM runs weekly (Friday) with 48h retention to free MinIO space.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta

from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException

logger = logging.getLogger(__name__)

BRONZE_TABLES = {
    "market_trade": "s3://market-data/bronze/market_trade",
    "market_quote": "s3://market-data/bronze/market_quote",
}


@dag(
    dag_id="dag_compact_bronze",
    schedule="0 16 * * 1-5",
    start_date=None,
    catchup=False,
    tags=["marketpulse", "delta-lake", "maintenance"],
    doc_md=__doc__,
)
def dag_compact_bronze():

    @task()
    def check_trading_day() -> bool:
        from utils.db import is_trading_day
        today = (datetime.now(timezone.utc) + timedelta(hours=7)).date()
        result = is_trading_day(today)
        logger.info("[CHECK] %s is_trading_day=%s", today, result)
        if not result:
            raise AirflowSkipException(f"Skipping: {today} is not a trading day")
        return result

    @task()
    def optimize_market_trade(is_trading: bool) -> dict:
        from utils.delta_writer import optimize_delta_table
        today = (datetime.now(timezone.utc) + timedelta(hours=7)).date().isoformat()
        return optimize_delta_table(BRONZE_TABLES["market_trade"], today, z_order_cols=["symbol"])

    @task()
    def optimize_market_quote(is_trading: bool) -> dict:
        from utils.delta_writer import optimize_delta_table
        today = (datetime.now(timezone.utc) + timedelta(hours=7)).date().isoformat()
        return optimize_delta_table(BRONZE_TABLES["market_quote"], today, z_order_cols=["symbol"])

    @task()
    def vacuum_market_trade(is_trading: bool) -> dict:
        """Run weekly VACUUM on market_trade (Fridays only)."""
        from utils.delta_writer import vacuum_delta_table
        now_ict = datetime.now(timezone.utc) + timedelta(hours=7)
        if now_ict.weekday() != 4:  # 4 = Friday
            logger.info("[SKIP] VACUUM only runs on Fridays (today=%s)", now_ict.strftime("%A"))
            return {"status": "skipped", "reason": "not Friday"}
        return vacuum_delta_table(BRONZE_TABLES["market_trade"], retention_hours=48)

    @task()
    def vacuum_market_quote(is_trading: bool) -> dict:
        """Run weekly VACUUM on market_quote (Fridays only)."""
        from utils.delta_writer import vacuum_delta_table
        now_ict = datetime.now(timezone.utc) + timedelta(hours=7)
        if now_ict.weekday() != 4:  # 4 = Friday
            logger.info("[SKIP] VACUUM only runs on Fridays (today=%s)", now_ict.strftime("%A"))
            return {"status": "skipped", "reason": "not Friday"}
        return vacuum_delta_table(BRONZE_TABLES["market_quote"], retention_hours=48)

    @task()
    def log_summary(
        trade_metrics: dict,
        quote_metrics: dict,
        vacuum_trade: dict,
        vacuum_quote: dict,
    ) -> None:
        logger.info("[DONE] market_trade compact: %s", trade_metrics)
        logger.info("[DONE] market_quote compact: %s", quote_metrics)
        logger.info("[DONE] market_trade vacuum:  %s", vacuum_trade)
        logger.info("[DONE] market_quote vacuum:  %s", vacuum_quote)

    # ── DAG wiring ────────────────────────────────────────────
    is_trading    = check_trading_day()
    trade_metrics = optimize_market_trade(is_trading)
    quote_metrics = optimize_market_quote(is_trading)
    vacuum_trade  = vacuum_market_trade(is_trading)
    vacuum_quote  = vacuum_market_quote(is_trading)
    log_summary(trade_metrics, quote_metrics, vacuum_trade, vacuum_quote)


dag_compact_bronze()
