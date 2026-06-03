"""
dag_secdef_sync — Daily sync of security definitions (ceiling/floor/reference prices).

Schedule: 07:45 ICT daily (Mon-Fri)

Tasks:
  1. check_trading_day      — skip if today is not in trading_calendar
  2. run_sync_secdef        — call sync_secdef.py --timeout 1800
  3. snapshot_to_delta      — snapshot today's rows -> Delta Lake Bronze
  4. verify                 — log final row counts
  5. trigger_instrument_delta — trigger dag_instrument_delta for change detection
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

from airflow.sdk import dag, task

SYNC_SECDEF_SCRIPT = str(Path("/opt/airflow/ingestion/handlers/sync_secdef.py"))
SECDEF_TIMEOUT     = 1800


def _today_ict():
    return (datetime.now(timezone.utc) + timedelta(hours=7)).date()


@dag(
    dag_id="dag_secdef_sync",
    schedule="45 7 * * 1-5",
    start_date=None,
    catchup=False,
    tags=["marketpulse", "reference", "secdef"],
    doc_md=__doc__,
)
def dag_secdef_sync():

    @task()
    def check_trading_day() -> bool:
        from utils.db import is_trading_day
        today = _today_ict()
        if not is_trading_day(today):
            raise Exception(f"Skipping: {today} is not a trading day")
        return True

    @task()
    def run_sync_secdef(is_trading: bool) -> int:
        from utils.dnse_helpers import run_sync_secdef as _run
        return _run(SYNC_SECDEF_SCRIPT, SECDEF_TIMEOUT)

    @task()
    def snapshot_to_delta(n_upserted: int) -> int:
        from utils.delta_writer import snapshot_secdef_to_delta
        return snapshot_secdef_to_delta(_today_ict())

    @task()
    def verify(n_db: int, n_delta: int) -> None:
        from utils.db import count_secdef_today
        import logging
        today = _today_ict()
        db_total = count_secdef_today(today)
        logging.getLogger(__name__).info(
            "[DONE] trading_date=%s | DB rows: %d | Delta rows written: %d",
            today, db_total, n_delta,
        )

    @task()
    def trigger_instrument_delta(n_db: int, n_delta: int) -> None:
        from utils.dnse_helpers import trigger_dag
        trigger_dag("dag_instrument_delta")

    # ── DAG wiring ────────────────────────────────────────────────
    is_trading = check_trading_day()
    n_db       = run_sync_secdef(is_trading)
    n_delta    = snapshot_to_delta(n_db)
    verify(n_db, n_delta)
    trigger_instrument_delta(n_db, n_delta)


dag_secdef_sync()
