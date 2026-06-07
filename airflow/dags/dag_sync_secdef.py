"""
dag_secdef_sync — Daily sync of security definitions (ceiling/floor/reference prices).

Schedule: 07:55 ICT daily (Mon-Fri)

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
SECDEF_TIMEOUT     = 900


def _today_ict():
    return (datetime.now(timezone.utc) + timedelta(hours=7)).date()


@dag(
    dag_id="dag_secdef_sync",
    schedule="55 7 * * 1-5",
    start_date=None,
    catchup=False,
    tags=["marketpulse", "reference", "secdef"],
    doc_md=__doc__,
)
def dag_secdef_sync():

    @task()
    def check_trading_day() -> bool:
        import logging
        from utils.db import is_trading_day

        log = logging.getLogger(__name__)
        today = _today_ict()

        # ── Check 1: trading calendar ─────────────────────────────
        if not is_trading_day(today):
            raise Exception(f"Skipping: {today} is not a trading day")

        # ── Check 2: time-window guard ────────────────────────────
        MAX_LAG_MINUTES = 15    # 07:55 + 15 min = cutoff at 08:10 ICT
        SCHEDULED_HOUR  = 7
        SCHEDULED_MIN   = 55

        now_ict = datetime.now(timezone.utc) + timedelta(hours=7)
        cutoff  = now_ict.replace(
            hour=SCHEDULED_HOUR, minute=SCHEDULED_MIN, second=0, microsecond=0
        ) + timedelta(minutes=MAX_LAG_MINUTES)

        if now_ict > cutoff:
            log.warning(
                "[SKIP] dag_secdef_sync triggered at %s ICT, but cutoff is %s ICT. "
                "Run is too late — secdef data would be unreliable. "
                "Manually trigger this DAG before %s to sync properly.",
                now_ict.strftime("%H:%M"),
                cutoff.strftime("%H:%M"),
                cutoff.strftime("%H:%M"),
            )
            raise Exception(
                f"Skipping: run time {now_ict.strftime('%H:%M')} ICT is past the "
                f"secdef window (cutoff {cutoff.strftime('%H:%M')} ICT)"
            )

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
