"""
dag_cleanup_timescaledb — Daily database maintenance.

Schedule: 16:30 ICT daily (Mon–Fri) = 09:30 UTC

Tasks:
  1. check_trading_day    — skip on non-trading days
  2. cleanup_secdef       — DELETE security_definition rows older than 90 days
  3. vacuum_analyze       — VACUUM ANALYZE hot tables (runs in parallel with 4 & 5)
  4. chunk_health         — verify TimescaleDB retention/compression jobs
  5. table_sizes          — log storage growth
"""

from __future__ import annotations

from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException



@dag(
    dag_id="dag_cleanup_timescaledb",
    schedule="30 16 * * 1-5",
    start_date=None,
    catchup=False,
    tags=["marketpulse", "maintenance", "timescaledb"],
    doc_md=__doc__,
)
def dag_cleanup_timescaledb():

    @task()
    def check_trading_day() -> bool:
        from datetime import datetime, timezone, timedelta
        from utils.db import is_trading_day
        import logging
        today = (datetime.now(timezone.utc) + timedelta(hours=7)).date()
        result = is_trading_day(today)
        logging.getLogger(__name__).info("[CHECK] %s is_trading_day=%s", today, result)
        if not result:
            raise AirflowSkipException(f"Skipping: {today} is not a trading day")
        return result

    @task()
    def cleanup_secdef(is_trading: bool) -> int:
        from utils.db_maintenance import cleanup_old_secdef
        return cleanup_old_secdef(retention_days=90)

    @task()
    def vacuum_analyze(is_trading: bool) -> None:
        from utils.db_maintenance import vacuum_analyze_tables
        vacuum_analyze_tables()

    @task()
    def chunk_health(is_trading: bool) -> None:
        from utils.db_maintenance import check_timescaledb_job_health
        check_timescaledb_job_health()

    @task()
    def table_sizes(is_trading: bool) -> None:
        from utils.db_maintenance import log_table_sizes
        log_table_sizes()

    # ── DAG wiring ────────────────────────────────────────────────
    is_trading = check_trading_day()
    cleanup_secdef(is_trading)
    # vacuum, health check, sizes run in parallel
    vacuum_analyze(is_trading)
    chunk_health(is_trading)
    table_sizes(is_trading)


dag_cleanup_timescaledb()
