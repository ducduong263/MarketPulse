"""
dag_data_quality — Weekly data quality checks.

Schedule: Every Monday 02:00 UTC (09:00 ICT)

Checks:
  1. record_counts   — rows per table vs expected minimums (last 7 days)
  2. spread_sanity   — bid_price1 < ask_price1 in order_book_l2
  3. ts_gaps         — gaps > 10 min in market_trade during trading hours
  4. delta_health    — Delta Lake table file counts
  5. summary         — aggregated report (informational, does not fail DAG)
"""

from __future__ import annotations

from airflow.sdk import dag, task


@dag(
    dag_id="dag_data_quality",
    schedule="0 18 * * 5",
    start_date=None,
    catchup=False,
    tags=["marketpulse", "data-quality"],
    doc_md=__doc__,
)
def dag_data_quality():

    @task()
    def record_counts() -> dict:
        from utils.data_quality_checks import check_record_counts
        return check_record_counts()

    @task()
    def spread_sanity() -> dict:
        from utils.data_quality_checks import check_spread_sanity
        return check_spread_sanity()

    @task()
    def ts_gaps() -> dict:
        from utils.data_quality_checks import check_ts_gaps
        return check_ts_gaps()

    @task()
    def delta_health() -> dict:
        from utils.data_quality_checks import check_delta_lake_health
        return check_delta_lake_health()

    @task()
    def summary(counts: dict, spread: dict, gaps: dict, delta: dict) -> None:
        from utils.data_quality_checks import build_summary_report
        build_summary_report(counts, spread, gaps, delta)

    # ── DAG wiring ────────────────────────────────────────────────
    summary(record_counts(), spread_sanity(), ts_gaps(), delta_health())


dag_data_quality()
