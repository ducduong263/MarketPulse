"""
dag_backfill_silver — Manual Silver Layer OHLC backfill.

Backfills Silver data for all trading days between from_date and to_date
by reading Bronze data that is already available on MinIO.

Required Conf params (pass via Airflow UI "Trigger w/ config"):
  from_date : "YYYY-MM-DD"   Start date (ICT), inclusive
  to_date   : "YYYY-MM-DD"   End date (ICT), inclusive

Optional Conf params:
  symbols     : ["VCB", ...]  default: all symbols found in Bronze
  resolutions : ["1min","1D"] default: both
  overwrite   : true/false    default: false

Example conf JSON:
  {"from_date": "2024-01-02", "to_date": "2026-07-01", "overwrite": true}
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from airflow.models.param import Param

logger = logging.getLogger(__name__)


def _get_trading_dates(from_date: str, to_date: str) -> list[str]:
    """
    Smart trading date enumeration using trading_calendar as source of truth.

    Strategy:
    - Query DB for: min(trading_date), and all confirmed trading dates in [from_date, to_date].
    - Dates BEFORE min(trading_date): calendar has no data for this period, so include all
      Mon-Fri (we cannot know which were holidays). Bronze will simply be empty for true
      holidays and the transform will produce 0 rows (safe to run).
    - Dates WITHIN [min(trading_date), to_date]: only include dates confirmed in trading_calendar.
      Dates not in the table are real holidays -- skip entirely.
    - Falls back to all Mon-Fri if DB is unavailable.
    """
    import sys
    from datetime import date as date_cls
    from pathlib import Path

    for root in [Path("/opt/airflow"), Path(__file__).resolve().parents[2]]:
        if root.exists() and str(root) not in sys.path:
            sys.path.insert(0, str(root))

    start = date_cls.fromisoformat(from_date)
    end   = date_cls.fromisoformat(to_date)

    # ── Try to load calendar from DB ─────────────────────────────────────────
    min_cal_date = None
    known_trading_days: set[date_cls] = set()

    try:
        from utils.db import get_db_conn
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                # One query: get min date + all confirmed dates in range
                cur.execute(
                    """
                    SELECT
                        MIN(trading_date) AS min_date,
                        ARRAY_AGG(trading_date) FILTER (
                            WHERE trading_date BETWEEN %s AND %s
                        ) AS dates_in_range
                    FROM trading_calendar
                    """,
                    (from_date, to_date),
                )
                row = cur.fetchone()
                if row and row[0]:
                    min_cal_date = row[0]  # a date object from psycopg2
                    known_trading_days = set(row[1]) if row[1] else set()

        logger.info(
            "[BACKFILL] trading_calendar: min_date=%s, confirmed_days_in_range=%d",
            min_cal_date, len(known_trading_days),
        )
    except Exception as exc:
        logger.warning("[BACKFILL] Cannot read trading_calendar (%s) — falling back to Mon-Fri", exc)

    # ── Build date list ───────────────────────────────────────────────────────
    result = []
    cur_date = start
    while cur_date <= end:
        if cur_date.weekday() < 5:  # Mon-Fri only (never include weekends)
            if min_cal_date is None:
                # No calendar at all: include all weekdays
                result.append(cur_date.isoformat())
            elif cur_date < min_cal_date:
                # Before calendar coverage: include as weekday (cannot verify holidays)
                result.append(cur_date.isoformat())
            elif cur_date in known_trading_days:
                # Within calendar: confirmed trading day
                result.append(cur_date.isoformat())
            # else: within calendar range but not a trading day = holiday, skip silently
        cur_date += timedelta(days=1)

    logger.info(
        "[BACKFILL] Date range %s -> %s: %d trading dates to process "
        "(min_cal_date=%s, skipped holidays within calendar range)",
        from_date, to_date, len(result), min_cal_date,
    )
    return result



@dag(
    dag_id="dag_backfill_silver",
    schedule=None,
    start_date=None,
    catchup=False,
    tags=["marketpulse", "silver", "backfill"],
    doc_md=__doc__,
    params={
        "from_date": Param(
            default="2024-01-02",
            type="string",
            format="date",
            title="From Date (ICT)",
            description="Select start date from calendar (inclusive)",
        ),
        "to_date": Param(
            default="",
            type=["null", "string"],
            format="date",
            title="To Date (ICT)",
            description="Select end date from calendar (inclusive). Leave empty for today.",
        ),
        "symbols": Param(
            default="",
            type=["null", "string"],
            title="Symbols",
            description="Comma-separated symbols (e.g. VCB,VN30F1M). Empty/Null = all symbols found in Bronze.",
        ),
        "resolutions": Param(
            default="",
            type=["null", "string"],
            title="Resolutions",
            description="Comma-separated resolutions (e.g. 1min,1D). Empty/Null = both.",
        ),
        "overwrite": Param(
            default=False,
            type="boolean",
            title="Overwrite Existing Data",
            description="Set to true to delete existing Silver rows before writing new ones.",
        ),
    },
)
def dag_backfill_silver():

    @task()
    def resolve_and_validate(**context) -> dict:
        """Validate input params and return resolved config."""
        params    = context.get("params", {})
        from_date = str(params.get("from_date") or "2024-01-02").strip()
        to_date   = str(params.get("to_date") or "").strip()
        if not to_date or to_date == "None":
            to_date = (datetime.now(timezone.utc) + timedelta(hours=7)).date().isoformat()

        # Parse comma-separated strings to list of strings
        sym_str = str(params.get("symbols") or "").strip()
        symbols = [s.strip() for s in sym_str.split(",") if s.strip()] if sym_str else None

        res_str = str(params.get("resolutions") or "").strip()
        resolutions = [r.strip() for r in res_str.split(",") if r.strip()] if res_str else None

        overwrite = bool(params.get("overwrite") or False)

        trading_dates = _get_trading_dates(from_date, to_date)
        logger.info(
            "[BACKFILL] from=%s to=%s dates=%d symbols=%s resolutions=%s overwrite=%s",
            from_date, to_date, len(trading_dates), symbols, resolutions, overwrite,
        )
        return {
            "trading_dates": trading_dates,
            "symbols":       symbols,
            "resolutions":   resolutions,
            "overwrite":     overwrite,
        }


    @task()
    def run_backfill(config: dict) -> dict:
        """
        Iterate over all trading dates and run Silver transform for each day.
        Runs sequentially to avoid overwhelming MinIO.
        """
        import sys
        from pathlib import Path
        for root in [Path("/opt/airflow"), Path(__file__).resolve().parents[2]]:
            if root.exists() and str(root) not in sys.path:
                sys.path.insert(0, str(root))

        from transform.silver.transform_ohlc import run_silver_transform

        trading_dates = config["trading_dates"]
        symbols       = config["symbols"]
        resolutions   = config["resolutions"]
        overwrite     = config["overwrite"]

        total_stats: dict = {}
        failed_dates: list[str] = []

        for i, date_str in enumerate(trading_dates, 1):
            logger.info("[BACKFILL] [%d/%d] Processing %s ...", i, len(trading_dates), date_str)
            try:
                day_stats = run_silver_transform(
                    date_str=date_str,
                    symbols=symbols,
                    resolutions=resolutions,
                    overwrite=overwrite,
                )
                # Accumulate totals
                for res, s in day_stats.items():
                    if res not in total_stats:
                        total_stats[res] = {"days": 0, "rows_written": 0, "skipped": 0}
                    total_stats[res]["days"]         += 1
                    total_stats[res]["rows_written"]  += s.get("rows_written", 0)
                    total_stats[res]["skipped"]       += s.get("skipped", 0)
            except Exception as exc:
                logger.error("[BACKFILL] FAILED for date=%s: %s", date_str, exc, exc_info=True)
                failed_dates.append(date_str)
                continue

        logger.info("[BACKFILL] Complete. Total stats: %s", total_stats)
        if failed_dates:
            logger.warning("[BACKFILL] Failed dates (%d): %s", len(failed_dates), failed_dates)

        return {"stats": total_stats, "failed_dates": failed_dates}

    @task()
    def log_summary(result: dict) -> None:
        stats        = result.get("stats", {})
        failed_dates = result.get("failed_dates", [])
        for res, s in stats.items():
            logger.info(
                "[BACKFILL DONE] %s: days=%d rows_written=%d skipped=%d",
                res, s["days"], s["rows_written"], s["skipped"],
            )
        if failed_dates:
            logger.warning("[BACKFILL] %d days failed: %s", len(failed_dates), failed_dates)
        else:
            logger.info("[BACKFILL] All dates processed successfully.")

    # ── DAG wiring ─────────────────────────────────────────────────────────────
    config = resolve_and_validate()
    result = run_backfill(config)
    log_summary(result)


dag_backfill_silver()
