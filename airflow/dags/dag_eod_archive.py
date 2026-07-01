"""
dag_eod_archive — End-of-day archive to Delta Lake Bronze.

Schedule: 15:15 ICT daily (Mon–Fri) = 08:15 UTC

Archives tables without real-time archivers:
  - foreign_investor  → Kafka topic market.foreign-investor (from 09:00 ICT)
  - market_index      → Kafka topic market.index (from 09:00 ICT)

market_trade and market_quote are archived in real-time by Docker services.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta

from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)


@dag(
    dag_id="dag_eod_archive",
    schedule="30 15 * * 1-5",
    start_date=None,
    catchup=False,
    tags=["marketpulse", "archive", "delta-lake"],
    doc_md=__doc__,
)
def dag_eod_archive():

    @task()
    def check_trading_day() -> bool:
        from utils.db import is_trading_day
        today = (datetime.now(timezone.utc) + timedelta(hours=7)).date()
        result = is_trading_day(today)
        logger.info("[CHECK] %s is_trading_day=%s", today, result)
        if not result:
            raise AirflowSkipException(f"{today} is not a trading day")
        return result

    @task()
    def archive_foreign_investor(is_trading: bool) -> int:
        from utils.eod_archive_helpers import archive_topic_to_delta
        return archive_topic_to_delta(
            topic       = "market.foreign-investor",
            schema_file = "foreign_investor.avsc",
            delta_uri   = "s3://market-data/bronze/foreign_investor",
            ts_cols     = ["exchange_ts", "producer_ts", "ingested_ts"],
            group_id    = "eod-archive-foreign-investor",
        )

    @task()
    def archive_market_index(is_trading: bool) -> int:
        from utils.eod_archive_helpers import archive_topic_to_delta
        return archive_topic_to_delta(
            topic       = "market.index",
            schema_file = "market_index.avsc",
            delta_uri   = "s3://market-data/bronze/market_index",
            ts_cols     = ["exchange_ts", "dnse_ts", "producer_ts", "ingested_ts"],
            group_id    = "eod-archive-market-index",
        )

    @task()
    def archive_ohlc_rest_api(is_trading: bool) -> dict:
        import sys
        from pathlib import Path
        possible_roots = [
            Path(__file__).resolve().parents[2],  # Host path
            Path("/opt/airflow"),                  # Container path
        ]
        for root in possible_roots:
            if root.exists() and str(root) not in sys.path:
                sys.path.insert(0, str(root))

        from ingestion.handlers.backfill_ohlc import run_eod_ohlc
        # Run EOD OHLC (defaults to today and resolutions ["1", "1D"])
        return run_eod_ohlc()

    @task()
    def log_summary(n_fi: int, n_index: int, ohlc_stats: dict) -> None:
        today_str = (datetime.now(timezone.utc) + timedelta(hours=7)).date().isoformat()
        logger.info(
            "[EOD ARCHIVE DONE] %s: foreign_investor=%d rows, market_index=%d rows, ohlc_resolutions=%s",
            today_str, n_fi, n_index, list(ohlc_stats.keys()) if ohlc_stats else [],
        )

    # ── DAG wiring ────────────────────────────────────────────────
    is_trading  = check_trading_day()
    n_fi        = archive_foreign_investor(is_trading)
    n_index     = archive_market_index(is_trading)
    ohlc_stats  = archive_ohlc_rest_api(is_trading)
    summary     = log_summary(n_fi, n_index, ohlc_stats)

    # Trigger Silver transform after EOD Bronze is complete
    today_str = (datetime.now(timezone.utc) + timedelta(hours=7)).date().isoformat()
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_ohlc",
        trigger_dag_id="dag_silver_ohlc",
        conf={"date": today_str},
        wait_for_completion=False,
        reset_dag_run=True,
    )
    summary >> trigger_silver


dag_eod_archive()
