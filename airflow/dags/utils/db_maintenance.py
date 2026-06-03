"""
utils/db_maintenance.py — TimescaleDB maintenance operations for MarketPulse DAGs.

Provides:
- cleanup_old_secdef(retention_days)  : DELETE old security_definition rows
- vacuum_analyze_tables(tables)       : VACUUM ANALYZE hot tables (autocommit)
- check_timescaledb_job_health()      : log TimescaleDB background job status
- log_table_sizes()                   : log hypertable + regular table sizes
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone, timedelta

import psycopg2

logger = logging.getLogger(__name__)

# Tables to VACUUM ANALYZE after each session
HOT_TABLES = [
    "market_trade",
    "order_book_l2",
    "foreign_investor",
    "market_index",
    "security_definition",
]


def _autocommit_conn():
    """Open a psycopg2 connection in autocommit mode (required for VACUUM)."""
    conn = psycopg2.connect(
        host=os.getenv("postgres_host",     "timescaledb"),
        port=int(os.getenv("postgres_port", "5432")),
        dbname=os.getenv("postgres_db",     "market_data"),
        user=os.getenv("postgres_user",     "marketpulse"),
        password=os.getenv("postgres_password", "mp_secret_2026"),
    )
    conn.autocommit = True
    return conn


def cleanup_old_secdef(retention_days: int = 90) -> int:
    """
    DELETE rows from security_definition older than retention_days.

    security_definition is a regular table (not hypertable), so it needs
    explicit cleanup — no automatic TimescaleDB retention policy applies.

    Args:
        retention_days: Rows older than this are deleted. Default: 90.

    Returns:
        Number of rows deleted.
    """
    from .db import get_db_conn

    cutoff = (datetime.now(timezone.utc) + timedelta(hours=7)).date() - timedelta(days=retention_days)
    logger.info("Deleting security_definition rows before %s (retention=%d days)...", cutoff, retention_days)

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM security_definition WHERE trading_date < %s", (cutoff,))
            deleted = cur.rowcount
        conn.commit()

    logger.info("[SECDEF] Deleted %d rows older than %s", deleted, cutoff)
    return deleted


def vacuum_analyze_tables(tables: list[str] | None = None) -> None:
    """
    Run VACUUM ANALYZE on a list of tables.

    Must use autocommit=True (VACUUM cannot run inside a transaction block).

    Args:
        tables: Table names to vacuum. Defaults to HOT_TABLES.
    """
    if tables is None:
        tables = HOT_TABLES

    conn = _autocommit_conn()
    try:
        with conn.cursor() as cur:
            for table in tables:
                logger.info("VACUUM ANALYZE %s ...", table)
                cur.execute(f"VACUUM ANALYZE {table}")
                logger.info("  Done: %s", table)
    finally:
        conn.close()


def check_timescaledb_job_health() -> None:
    """
    Query timescaledb_information.jobs and log status of retention/compression jobs.
    Logs a warning for any job with recorded failures.
    """
    from .db import get_db_conn

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT j.job_id, j.proc_name, j.schedule_interval,
                       s.last_run_status, s.last_successful_finish,
                       s.total_runs, s.total_failures
                FROM timescaledb_information.jobs j
                LEFT JOIN timescaledb_information.job_stats s ON j.job_id = s.job_id
                WHERE j.proc_schema = '_timescaledb_internal'
                   OR j.proc_name LIKE '%retention%'
                   OR j.proc_name LIKE '%compress%'
                ORDER BY j.job_id
            """)
            rows = cur.fetchall()

    if not rows:
        logger.warning("[HEALTH] No TimescaleDB jobs found")
        return

    failed = []
    for job_id, proc_name, interval, status, last_ok, total_runs, total_fail in rows:
        logger.info(
            "  Job %s (%s): status=%s, last_ok=%s, runs=%s, failures=%s",
            job_id, proc_name, status, last_ok, total_runs, total_fail,
        )
        if total_fail:
            failed.append(f"Job {job_id} ({proc_name}): {total_fail} failures")

    if failed:
        logger.warning("[HEALTH] %d job(s) with failures: %s", len(failed), failed)
    else:
        logger.info("[HEALTH] All TimescaleDB jobs healthy")


def log_table_sizes() -> None:
    """
    Log current storage size for all hypertables and key regular tables.
    Useful for tracking storage growth over time.
    """
    from .db import get_db_conn

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT hypertable_name,
                       pg_size_pretty(total_bytes)  AS total_size,
                       pg_size_pretty(table_bytes)  AS heap_size,
                       pg_size_pretty(index_bytes)  AS index_size
                FROM (
                    SELECT hypertable_name,
                           (hypertable_detailed_size(hypertable_name::regclass)).*
                    FROM timescaledb_information.hypertables
                ) sub
                ORDER BY total_bytes DESC
            """)
            hyper_rows = cur.fetchall()

            cur.execute("""
                SELECT tablename,
                       pg_size_pretty(pg_total_relation_size(tablename::regclass)) AS total_size
                FROM pg_tables
                WHERE schemaname = 'public'
                  AND tablename IN ('security_definition', 'trading_calendar', 'instrument_master')
                ORDER BY pg_total_relation_size(tablename::regclass) DESC
            """)
            regular_rows = cur.fetchall()

    logger.info("[SIZES] Hypertable sizes:")
    for name, total, heap, index in hyper_rows:
        logger.info("  %s: total=%s, heap=%s, index=%s", name, total, heap, index)

    logger.info("[SIZES] Regular table sizes:")
    for name, total in regular_rows:
        logger.info("  %s: %s", name, total)
