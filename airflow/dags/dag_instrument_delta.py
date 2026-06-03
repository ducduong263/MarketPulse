"""
dag_instrument_delta — Daily instrument reconciliation vs today's security_definition.

Schedule:
  - Triggered by dag_secdef_sync after each successful sync
  - Can also be triggered manually

Tasks:
  1. detect_changes     — compare instrument_master (is_active=True) vs secdef today
                          first-run (empty IM): bulk fetch all instruments
                          normal: find new symbols + symbols to deactivate
  2. fetch_and_upsert   — upsert new/returned symbols from DNSE API
                          fallback: upsert minimal row from secdef if API misses symbol
  3. deactivate         — set is_active=False for symbols not in today's secdef
                          (handles both newly delisted AND long-stale historical symbols)
  4. log_summary        — report what changed
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

from airflow.sdk import dag, task


def _today_ict():
    return (datetime.now(timezone.utc) + timedelta(hours=7)).date()


@dag(
    dag_id="dag_instrument_delta",
    schedule=None,
    start_date=None,
    catchup=False,
    tags=["marketpulse", "reference", "instruments"],
    doc_md=__doc__,
)
def dag_instrument_delta():

    @task()
    def detect_changes() -> dict:
        from utils.instrument_delta import detect_instrument_changes
        return detect_instrument_changes(_today_ict())

    @task()
    def fetch_and_upsert(changes: dict) -> int:
        from utils.instrument_delta import upsert_new_instruments
        return upsert_new_instruments(changes)

    @task()
    def deactivate(changes: dict) -> int:
        from utils.instrument_delta import deactivate_gone_instruments
        return deactivate_gone_instruments(changes)

    @task()
    def log_summary(n_added: int, n_deactivated: int, changes: dict) -> None:
        from utils.instrument_delta import log_delta_summary
        log_delta_summary(n_added, n_deactivated, changes)

    # ── DAG wiring ────────────────────────────────────────────────
    changes    = detect_changes()
    n_added    = fetch_and_upsert(changes)
    n_gone     = deactivate(changes)
    log_summary(n_added, n_gone, changes)


dag_instrument_delta()
