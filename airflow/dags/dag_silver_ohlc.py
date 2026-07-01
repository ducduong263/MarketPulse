"""
dag_silver_ohlc — Daily Silver Layer OHLC transform.

Schedule: None (triggered by dag_eod_archive after EOD Bronze is ready).
Can also be triggered manually from Airflow UI with optional conf params.

Conf params (optional):
  date        : "YYYY-MM-DD" (ICT) — default: today ICT
  symbols     : ["VCB", "VN30F1M", ...]  — default: all
  resolutions : ["1min", "1D"]            — default: both
  overwrite   : true/false                — default: false
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

logger = logging.getLogger(__name__)


@dag(
    dag_id="dag_silver_ohlc",
    schedule=None,   # Triggered by dag_eod_archive (or manually)
    start_date=None,
    catchup=False,
    tags=["marketpulse", "silver", "delta-lake"],
    doc_md=__doc__,
    params={
        "date":        {"type": "string",  "default": "", "description": "YYYY-MM-DD (ICT). Empty = today"},
        "symbols":     {"type": "array",   "default": [], "description": "Symbols. Empty = all"},
        "resolutions": {"type": "array",   "default": [], "description": "['1min','1D']. Empty = both"},
        "overwrite":   {"type": "boolean", "default": False},
    },
)
def dag_silver_ohlc():

    @task()
    def resolve_params(**context) -> dict:
        """Resolve run parameters — fallback to sensible defaults."""
        params = context.get("params", {})
        date_str = params.get("date", "").strip()
        if not date_str:
            date_str = (datetime.now(timezone.utc) + timedelta(hours=7)).date().isoformat()

        # dag_eod_archive may forward logical_date via TriggerDagRunOperator conf
        conf = context["dag_run"].conf or {}
        if not date_str and "date" in conf:
            date_str = conf["date"]

        symbols     = params.get("symbols") or conf.get("symbols") or None
        resolutions = params.get("resolutions") or conf.get("resolutions") or None
        overwrite   = bool(params.get("overwrite") or conf.get("overwrite") or False)

        logger.info(
            "[SILVER DAG] date=%s symbols=%s resolutions=%s overwrite=%s",
            date_str, symbols, resolutions, overwrite,
        )
        return {
            "date":        date_str,
            "symbols":     symbols,
            "resolutions": resolutions,
            "overwrite":   overwrite,
        }

    @task()
    def transform_1min(run_params: dict) -> dict:
        """Transform 1-minute candles: fill missing bars per asset time spine."""
        import sys
        from pathlib import Path
        for root in [Path("/opt/airflow"), Path(__file__).resolve().parents[2]]:
            if root.exists() and str(root) not in sys.path:
                sys.path.insert(0, str(root))

        from transform.silver.transform_ohlc import run_silver_transform

        stats = run_silver_transform(
            date_str=run_params["date"],
            symbols=run_params["symbols"],
            resolutions=["1min"],
            overwrite=run_params["overwrite"],
        )
        logger.info("[SILVER 1min] Stats: %s", stats)
        return stats.get("1min", {})

    @task()
    def transform_1D(run_params: dict) -> dict:
        """Transform daily candles: enrich with metadata (no fill needed)."""
        import sys
        from pathlib import Path
        for root in [Path("/opt/airflow"), Path(__file__).resolve().parents[2]]:
            if root.exists() and str(root) not in sys.path:
                sys.path.insert(0, str(root))

        from transform.silver.transform_ohlc import run_silver_transform

        stats = run_silver_transform(
            date_str=run_params["date"],
            symbols=run_params["symbols"],
            resolutions=["1D"],
            overwrite=run_params["overwrite"],
        )
        logger.info("[SILVER 1D] Stats: %s", stats)
        return stats.get("1D", {})

    @task()
    def log_summary(stats_1min: dict, stats_1D: dict, run_params: dict) -> None:
        logger.info(
            "[SILVER DONE] date=%s | 1min: symbols=%d rows=%d skipped=%d "
            "| 1D: symbols=%d rows=%d skipped=%d",
            run_params["date"],
            stats_1min.get("symbols", 0), stats_1min.get("rows_written", 0), stats_1min.get("skipped", 0),
            stats_1D.get("symbols", 0),   stats_1D.get("rows_written", 0),   stats_1D.get("skipped", 0),
        )

    # ── DAG wiring ─────────────────────────────────────────────────────────────
    run_params  = resolve_params()
    s1          = transform_1min(run_params)
    sD          = transform_1D(run_params)
    log_summary(s1, sD, run_params)


dag_silver_ohlc()
