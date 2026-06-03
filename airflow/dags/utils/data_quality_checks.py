"""
utils/data_quality_checks.py — Data quality check functions for MarketPulse.

Each function runs one category of checks and returns a result dict
with keys: 'issues' (list[str]) and check-specific metrics.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

MIN_ROWS_PER_DAY = {
    "market_trade":     1_000,
    "order_book_l2":    1_000,
    "market_index":     500,
    "foreign_investor": 10,
}

_TS_COL = {
    "market_trade":     "exchange_ts",
    "order_book_l2":    "exchange_ts",
    "market_index":     "exchange_ts",
    "foreign_investor": "producer_ts",
}

DELTA_TABLES = {
    "market_trade": "s3://market-data/bronze/market_trade",
    "market_quote": "s3://market-data/bronze/market_quote",
}


def _storage_options() -> dict:
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    return {
        "AWS_ENDPOINT_URL":      f"http://{endpoint}",
        "AWS_ACCESS_KEY_ID":     os.getenv("minio_root_user",     "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("minio_root_password", "minioadmin"),
        "AWS_REGION":            "us-east-1",
        "AWS_ALLOW_HTTP":        "true",
    }


def _today_ict():
    return (datetime.now(timezone.utc) + timedelta(hours=7)).date()


# ── Check functions ───────────────────────────────────────────────────────────

def check_record_counts() -> dict:
    """
    Check row counts per table over the past 7 days.
    Flags any day below MIN_ROWS_PER_DAY threshold.

    Returns:
        Dict {table: {days_checked: int, issues: list[str]}}
    """
    from .db import get_db_conn

    week_ago = _today_ict() - timedelta(days=7)
    results = {}

    with get_db_conn() as conn:
        for table, min_rows in MIN_ROWS_PER_DAY.items():
            ts_col = _TS_COL.get(table, "ingested_ts")
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT DATE({ts_col} AT TIME ZONE 'Asia/Ho_Chi_Minh') AS trading_day,
                           COUNT(*) AS row_count
                    FROM {table}
                    WHERE {ts_col} >= %s
                    GROUP BY 1
                    ORDER BY 1
                """, (week_ago,))
                rows = cur.fetchall()

            issues = []
            for trading_day, count in rows:
                if count < min_rows:
                    msg = f"{trading_day}: {count} rows (min={min_rows})"
                    issues.append(msg)
                    logger.warning("[COUNT] %s %s", table, msg)
                else:
                    logger.info("[COUNT] %s %s: %d rows OK", table, trading_day, count)

            results[table] = {"days_checked": len(rows), "issues": issues}

    return results


def check_spread_sanity() -> dict:
    """
    Verify bid_price1 < ask_price1 in order_book_l2 over the past week.

    Returns:
        Dict {total_checked, invalid_spread, negative_spread, issues: list[str]}
    """
    from .db import get_db_conn

    week_ago = _today_ict() - timedelta(days=7)

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE bid_price1 IS NOT NULL AND ask_price1 IS NOT NULL),
                    COUNT(*) FILTER (WHERE bid_price1 >= ask_price1),
                    COUNT(*) FILTER (WHERE spread < 0)
                FROM order_book_l2
                WHERE exchange_ts >= %s
            """, (week_ago,))
            row = cur.fetchone()

    total, invalid, negative = row if row else (0, 0, 0)
    issues = []
    if invalid:
        issues.append(f"{invalid} rows with bid_price >= ask_price")
    if negative:
        issues.append(f"{negative} rows with negative spread")

    logger.info("[SPREAD] total=%s invalid=%s negative=%s", total, invalid, negative)
    return {
        "total_checked":  total   or 0,
        "invalid_spread": invalid or 0,
        "negative_spread":negative or 0,
        "issues":         issues,
    }


def check_ts_gaps() -> dict:
    """
    Detect gaps > 10 minutes in market_trade during trading hours (ICT)
    on the most recent completed trading day.

    Returns:
        Dict {date_checked, gaps_found, issues: list[str]}
    """
    from .db import get_db_conn

    yesterday = _today_ict() - timedelta(days=1)

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                WITH ticks AS (
                    SELECT exchange_ts,
                           LAG(exchange_ts) OVER (ORDER BY exchange_ts) AS prev_ts
                    FROM market_trade
                    WHERE exchange_ts >= %s::date
                      AND exchange_ts <  %s::date + INTERVAL '1 day'
                      AND (
                          EXTRACT(HOUR FROM exchange_ts AT TIME ZONE 'Asia/Ho_Chi_Minh') BETWEEN 9 AND 11
                          OR
                          EXTRACT(HOUR FROM exchange_ts AT TIME ZONE 'Asia/Ho_Chi_Minh') BETWEEN 13 AND 14
                      )
                )
                SELECT exchange_ts, prev_ts,
                       EXTRACT(EPOCH FROM (exchange_ts - prev_ts)) / 60.0 AS gap_minutes
                FROM ticks
                WHERE EXTRACT(EPOCH FROM (exchange_ts - prev_ts)) > 600
                ORDER BY gap_minutes DESC
                LIMIT 10
            """, (yesterday, yesterday))
            gaps = cur.fetchall()

    issues = []
    for ts, prev_ts, gap_min in gaps:
        msg = f"Gap of {gap_min:.1f} min between {prev_ts} and {ts}"
        logger.warning("[GAPS] %s", msg)
        issues.append(msg)

    logger.info("[GAPS] market_trade: %d gaps > 10min on %s", len(gaps), yesterday)
    return {"date_checked": str(yesterday), "gaps_found": len(gaps), "issues": issues}


def check_delta_lake_health() -> dict:
    """
    Check Delta Lake table health: verify tables are readable and log file counts.

    Returns:
        Dict {table: {file_count, status}}
    """
    from deltalake import DeltaTable

    opts = _storage_options()
    results = {}

    for name, uri in DELTA_TABLES.items():
        try:
            dt = DeltaTable(uri, storage_options=opts)
            n_files = len(dt.file_uris())
            logger.info("[DELTA] %s: %d Parquet files", name, n_files)
            results[name] = {"file_count": n_files, "status": "ok"}
        except Exception as e:
            logger.warning("[DELTA] %s: error — %s", name, e)
            results[name] = {"file_count": -1, "status": f"error: {e}"}

    return results


def build_summary_report(
    count_results: dict,
    spread_results: dict,
    gap_results:   dict,
    delta_results: dict,
) -> None:
    """
    Aggregate all check results and log a final report.
    Informational only — does not raise on issues.
    """
    all_issues: list[str] = []

    for table, info in count_results.items():
        all_issues.extend(f"[COUNT:{table}] {i}" for i in info.get("issues", []))

    all_issues.extend(f"[SPREAD] {i}" for i in spread_results.get("issues", []))
    all_issues.extend(f"[GAP] {i}"    for i in gap_results.get("issues", []))

    for table, info in delta_results.items():
        if info.get("status", "").startswith("error"):
            all_issues.append(f"[DELTA:{table}] {info['status']}")

    sep = "=" * 60
    logger.info(sep)
    logger.info("[DATA QUALITY REPORT]")
    logger.info("  Record count issues : %d", sum(1 for i in all_issues if "COUNT"  in i))
    logger.info("  Spread issues       : %d", sum(1 for i in all_issues if "SPREAD" in i))
    logger.info("  Gap issues          : %d", sum(1 for i in all_issues if "GAP"    in i))
    logger.info("  Delta Lake issues   : %d", sum(1 for i in all_issues if "DELTA"  in i))
    logger.info("  Total issues        : %d", len(all_issues))

    if all_issues:
        for issue in all_issues:
            logger.warning("  ISSUE: %s", issue)
        logger.warning("[DATA QUALITY] %d issue(s) found — review logs above", len(all_issues))
    else:
        logger.info("[DATA QUALITY] All checks passed!")
    logger.info(sep)
