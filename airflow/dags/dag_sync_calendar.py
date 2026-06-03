"""
dag_sync_calendar — Sync trading calendar from DNSE → TimescaleDB.

Schedule: Every Monday 01:00 ICT (18:00 UTC Sunday) + manual trigger.
Refreshes trading_calendar with the latest working dates from DNSE API.
Run manually anytime a public holiday is added/changed.
"""

from __future__ import annotations

from airflow.sdk import dag, task


@dag(
    dag_id="dag_sync_calendar",
    schedule="0 1 * * 1",  # Monday 01:00 ICT
    start_date=None,
    catchup=False,
    tags=["marketpulse", "reference", "calendar"],
    doc_md=__doc__,
)
def dag_sync_calendar():

    @task()
    def fetch() -> list[str]:
        from utils.dnse_helpers import get_normalized_working_dates
        return get_normalized_working_dates()

    @task()
    def upsert(dates: list[str]) -> int:
        from utils.db import upsert_trading_calendar
        return upsert_trading_calendar(dates)

    @task()
    def verify(n_inserted: int) -> None:
        from utils.db import verify_trading_calendar
        verify_trading_calendar(n_inserted)

    verify(upsert(fetch()))


dag_sync_calendar()
