"""
dag_secdef_sync — Daily sync of security definitions (ceiling/floor/reference prices).

Schedule: 07:50 ICT daily (Mon-Fri)

Tasks:
  1. check_trading_day           — skip if today is not in trading_calendar
  2. fetch_and_upsert_instruments — REST API get_instruments -> upsert instrument_master
                                    (ensures sync_secdef.py has data to query from DB)
  3. run_sync_secdef             — WebSocket subscribe ~1,871 priority symbols (STO/STX/DVX)
                                    Timeout 900s, data broadcast at 08:00 ICT
  4. snapshot_to_delta           — snapshot today's rows -> Delta Lake Bronze
  5. trigger_instrument_delta    — trigger dag_instrument_delta early (08:02 ICT)
                                    so realtime HOSE/HNX flows start on time
  6. run_export_secdef           — REST API fallback for UPCOM/HCX (~1,100 symbols)
                                    Runs in parallel with snapshot_to_delta, does not block trigger
                                    Rate limit: 3.8s/request, completes ~09:15 ICT
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException


SYNC_SECDEF_SCRIPT   = str(Path("/opt/airflow/ingestion/handlers/sync_secdef.py"))
EXPORT_SECDEF_SCRIPT = str(Path("/opt/airflow/ingestion/handlers/export_secdef.py"))
SECDEF_TIMEOUT       = 900


def _today_ict():
    return (datetime.now(timezone.utc) + timedelta(hours=7)).date()


@dag(
    dag_id="dag_secdef_sync",
    schedule="50 7 * * 1-5",
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

        # -- Check 1: trading calendar ------------------------------------
        if not is_trading_day(today):
            raise AirflowSkipException(f"Skipping: {today} is not a trading day")

        # -- Check 2: time-window guard -----------------------------------
        MAX_LAG_MINUTES = 20    # 07:50 + 20 min = cutoff at 08:10 ICT
        SCHEDULED_HOUR  = 7
        SCHEDULED_MIN   = 50

        now_ict = datetime.now(timezone.utc) + timedelta(hours=7)
        cutoff  = now_ict.replace(
            hour=SCHEDULED_HOUR, minute=SCHEDULED_MIN, second=0, microsecond=0
        ) + timedelta(minutes=MAX_LAG_MINUTES)

        if now_ict > cutoff:
            log.warning(
                "[SKIP] dag_secdef_sync triggered at %s ICT, but cutoff is %s ICT. "
                "Run is too late -- secdef data would be unreliable. "
                "Manually trigger this DAG before %s to sync properly.",
                now_ict.strftime("%H:%M"),
                cutoff.strftime("%H:%M"),
                cutoff.strftime("%H:%M"),
            )
            raise AirflowSkipException(
                f"Skipping: run time {now_ict.strftime('%H:%M')} ICT is past the "
                f"secdef window (cutoff {cutoff.strftime('%H:%M')} ICT)"
            )

        return True

    @task()
    def fetch_and_upsert_instruments(is_trading: bool) -> int:
        """
        Step 1: Call REST API get_instruments to update instrument_master.
        Ensures sync_secdef.py has the data it needs to query the symbols to subscribe.
        """
        import logging
        from utils.dnse_helpers import fetch_dnse_instruments, build_instrument_rows
        from utils.db import upsert_instrument_master

        log = logging.getLogger(__name__)
        instruments = fetch_dnse_instruments()  # bulk paginated fetch (all ~3000 symbols)
        rows = build_instrument_rows(instruments)
        n = upsert_instrument_master(rows)
        log.info("[instruments] Upserted %d instruments into instrument_master", n)
        return n

    @task()
    def run_sync_secdef(n_instruments: int) -> int:
        """
        Step 2: Subscribe to ~1,871 priority symbols (STO/STX/DVX) via WebSocket.
        Must run at 07:59 ICT to be ready for the 08:00 ICT broadcast.
        """
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
            "[DONE] trading_date=%s | DB rows (WSS): %d | Delta rows written: %d",
            today, db_total, n_delta,
        )

    @task()
    def trigger_instrument_delta(n_db: int, n_delta: int) -> None:
        """
        Trigger dag_instrument_delta early so realtime HOSE/HNX ingestion starts on time.
        Runs immediately after WSS sync completes (no need to wait for REST fallback).
        """
        from utils.dnse_helpers import trigger_dag
        trigger_dag("dag_instrument_delta")

    @task()
    def run_export_secdef(n_db: int) -> int:
        """
        Step 3: REST API fallback for ~1,100 remaining symbols (UPCOM/HCX + missed WSS symbols).
        Runs in parallel with snapshot_to_delta and trigger_instrument_delta.
        Rate limit: 3.8s/request => completes around 09:15 ICT.
        """
        from utils.dnse_helpers import run_export_secdef as _run
        return _run(EXPORT_SECDEF_SCRIPT)

    # -- DAG wiring -------------------------------------------------------
    # Step 1: Get instrument list (needed for sync_secdef.py to query DB)
    is_trading   = check_trading_day()
    n_instruments = fetch_and_upsert_instruments(is_trading)

    # Step 2: WSS sync for STO/STX/DVX (priority)
    n_db         = run_sync_secdef(n_instruments)

    # Step 3a: Snapshot to Delta + trigger realtime DAGs (no need to wait for REST API)
    n_delta      = snapshot_to_delta(n_db)
    verify(n_db, n_delta)
    trigger_instrument_delta(n_db, n_delta)

    # Step 3b: REST API fallback for UPCOM/HCX (runs in parallel, in background)
    # Depends on n_db to only fetch symbols that are actually missing after WSS sync completes (~08:02 ICT)
    run_export_secdef(n_db)


dag_secdef_sync()

