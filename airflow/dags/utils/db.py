"""
utils/db.py — Database helpers for MarketPulse Airflow DAGs.

Provides:
- get_db_conn()                                  : psycopg2 connection context manager
- is_trading_day(date)                           : check if date exists in trading_calendar
- upsert_trading_calendar()                      : replace trading_calendar with new date list
- upsert_instrument_master(rows)                 : bulk upsert instruments
- get_prev_trading_date(today)                   : return the previous trading date before today
- get_secdef_symbols(trading_date)               : set of symbols in security_definition for a date
- get_all_instrument_symbols()                   : set of all symbols in instrument_master (any is_active)
- get_active_instrument_symbols()                : set of symbols where is_active=True
- get_inactive_instrument_symbols()              : set of symbols where is_active=False
- get_active_instrument_symbols_by_market(mkt)  : set of active symbols filtered by market_id
- get_secdef_rows_for_symbols(syms, date)        : raw secdef rows for fallback upsert
- deactivate_instruments(symbols)               : mark symbols as is_active = false
- reactivate_instruments(symbols)               : mark symbols as is_active = true
- upsert_instrument_master(rows)                 : bulk upsert instruments
- enrich_final_trade_date(symbols, today)        : backfill final_trade_date from secdef for symbols with NULL
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from datetime import date as date_type, datetime, timezone, timedelta

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# ── Config (injected via Airflow env / docker-compose) ────────────────────────
DB_HOST     = os.getenv("postgres_host",     "timescaledb")
DB_PORT     = int(os.getenv("postgres_port", "5432"))
DB_NAME     = os.getenv("postgres_db",       "market_data")
DB_USER     = os.getenv("postgres_user",     "marketpulse")
DB_PASSWORD = os.getenv("postgres_password", "mp_secret_2026")


# ── Connection ────────────────────────────────────────────────────────────────

def _make_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    )


@contextmanager
def get_db_conn():
    """
    Context manager yielding a psycopg2 connection.
    Rolls back on exception; always closes on exit.

    Usage:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
            conn.commit()
    """
    conn = _make_conn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── trading_calendar ──────────────────────────────────────────────────────────

def is_trading_day(check_date: date_type | None = None) -> bool:
    """
    Return True if check_date exists in trading_calendar (= it is a trading day).

    Args:
        check_date: Date to check. Defaults to today ICT (UTC+7).

    Returns:
        True if trading day, False if not found or table is empty.
    """
    if check_date is None:
        check_date = (datetime.now(timezone.utc) + timedelta(hours=7)).date()

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM trading_calendar WHERE trading_date = %s)",
                (check_date,),
            )
            row = cur.fetchone()

    result = bool(row[0]) if row else False
    if not result:
        logger.warning(
            "Date %s not found in trading_calendar — treating as non-trading day", check_date
        )
    return result


def get_prev_trading_date(today: date_type) -> date_type | None:
    """
    Return the most recent trading date before today from trading_calendar.
    Returns None if today is the first ever trading day in the DB.

    Args:
        today: The reference date.

    Returns:
        Previous trading date, or None if not found.
    """
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(trading_date) FROM trading_calendar WHERE trading_date < %s",
                (today,),
            )
            row = cur.fetchone()

    prev = row[0] if row else None
    if prev is None:
        logger.info("No previous trading date found before %s — first run", today)
    return prev


def upsert_trading_calendar(dates: list[str]) -> int:
    """
    Replace trading_calendar with the given list of trading dates.

    Strategy: DELETE all rows, then INSERT fresh — ensures removed holidays
    are also cleaned up (e.g. government changes a public holiday mid-year).

    Args:
        dates: List of 'YYYY-MM-DD' strings.

    Returns:
        Number of rows inserted.
    """
    if not dates:
        logger.warning("upsert_trading_calendar called with empty list — skipping")
        return 0

    rows = [(d,) for d in dates]

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM trading_calendar")
            deleted = cur.rowcount
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO trading_calendar (trading_date) VALUES %s",
                rows,
                page_size=500,
            )
            inserted = len(rows)
        conn.commit()

    logger.info(
        "trading_calendar refreshed: deleted %d old rows, inserted %d new rows (%s → %s)",
        deleted, inserted, dates[0], dates[-1],
    )
    return inserted


def verify_trading_calendar(n_inserted: int) -> None:
    """
    Log current state of trading_calendar after an upsert.
    Raises if table is empty.
    """
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*), MIN(trading_date), MAX(trading_date) FROM trading_calendar"
            )
            row = cur.fetchone()

    total, first, last = (row or (0, None, None))
    logger.info(
        "[VERIFY] trading_calendar: %d rows in DB (inserted %d this run) | %s → %s",
        total, n_inserted, first, last,
    )
    if total == 0:
        raise ValueError("trading_calendar is empty after upsert — check DNSE API response")


# ── instrument_master ─────────────────────────────────────────────────────────

_UPSERT_INSTRUMENT_SQL = """
INSERT INTO instrument_master (
    symbol, market_id, security_group_id, symbol_type,
    listed_date, final_trade_date, short_name, full_name, index_name,
    is_active
) VALUES %s
ON CONFLICT (symbol, market_id) DO UPDATE SET
    security_group_id   = EXCLUDED.security_group_id,
    symbol_type         = EXCLUDED.symbol_type,
    listed_date         = EXCLUDED.listed_date,
    -- final_trade_date: only populated for futures (market_id='DVX' / security_group_id='FU').
    -- Always NULL for equities, ETFs, bonds — this is correct, not missing data.
    -- COALESCE preserves an existing non-NULL value when incoming row has NULL
    -- (e.g. when refreshing metadata from DNSE instruments API which omits this field).
    final_trade_date    = COALESCE(EXCLUDED.final_trade_date, instrument_master.final_trade_date),
    short_name          = EXCLUDED.short_name,
    full_name           = EXCLUDED.full_name,
    index_name          = EXCLUDED.index_name,
    is_active           = EXCLUDED.is_active,
    last_synced_ts      = clock_timestamp()
"""


def upsert_instrument_master(rows: list[tuple]) -> int:
    """
    Upsert instrument rows into instrument_master table.

    Args:
        rows: List of tuples from dnse_helpers.build_instrument_rows().

    Returns:
        Number of rows upserted.
    """
    if not rows:
        logger.warning("upsert_instrument_master called with empty rows — skipping")
        return 0

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur, _UPSERT_INSTRUMENT_SQL, rows, page_size=500
            )
        conn.commit()

    logger.info("Upserted %d rows into instrument_master", len(rows))
    return len(rows)


def get_secdef_symbols(trading_date: date_type) -> set[str]:
    """
    Return the set of distinct symbols in security_definition for a given trading_date.

    Args:
        trading_date: The date to query.

    Returns:
        Set of symbol strings (empty if no data for that date).
    """
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT symbol FROM security_definition WHERE trading_date = %s",
                (trading_date,),
            )
            rows = cur.fetchall()

    symbols = {r[0] for r in rows}
    logger.info("[SECDEF] %d symbols on %s", len(symbols), trading_date)
    return symbols


def get_all_instrument_symbols() -> set[str]:
    """
    Return the set of ALL symbols in instrument_master, regardless of is_active.
    Used to detect brand-new symbols never seen before.
    """
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT symbol FROM instrument_master")
            rows = cur.fetchall()
    symbols = {r[0] for r in rows}
    logger.info("[IM] %d total symbols in instrument_master", len(symbols))
    return symbols


def get_active_instrument_symbols() -> set[str]:
    """
    Return the set of symbols in instrument_master where is_active = True.
    Used to find stale symbols no longer in today's secdef.
    """
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT symbol FROM instrument_master WHERE is_active = true"
            )
            rows = cur.fetchall()
    symbols = {r[0] for r in rows}
    logger.info("[IM] %d active symbols in instrument_master", len(symbols))
    return symbols


def get_inactive_instrument_symbols() -> set[str]:
    """
    Return the set of symbols in instrument_master where is_active = False.
    Used to detect symbols that were previously deactivated but now reappear in secdef.
    """
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT symbol FROM instrument_master WHERE is_active = false"
            )
            rows = cur.fetchall()
    symbols = {r[0] for r in rows}
    logger.info("[IM] %d inactive symbols in instrument_master", len(symbols))
    return symbols


def get_active_instrument_symbols_by_market(market_id: str) -> set[str]:
    """
    Return the set of active symbols in instrument_master filtered by market_id.
    Used by refresh_dvx_metadata to get all active DVX contracts for re-fetching
    their metadata (symbol_type, short_name) from the DNSE API after roll events.

    Args:
        market_id: Market identifier, e.g. 'DVX' for derivatives.

    Returns:
        Set of symbol strings where is_active = True and market_id matches.
    """
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT symbol FROM instrument_master "
                "WHERE is_active = true AND market_id = %s",
                (market_id,),
            )
            rows = cur.fetchall()
    symbols = {r[0] for r in rows}
    logger.info("[IM] %d active symbols in instrument_master for market=%s", len(symbols), market_id)
    return symbols


def get_symbols_missing_from_instrument_master() -> set[str]:
    """
    Return the set of symbols present in security_definition but missing from instrument_master.
    """
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT symbol 
                FROM security_definition 
                WHERE symbol NOT IN (SELECT symbol FROM instrument_master)
                """
            )
            rows = cur.fetchall()
    symbols = {r[0] for r in rows}
    logger.info("[SECDEF] Found %d symbols missing from instrument_master", len(symbols))
    return symbols


def get_secdef_rows_for_symbols(
    symbols: list[str]
) -> list[tuple]:
    """
    Return the latest raw security_definition rows for specific symbols.
    Used as fallback data source when the DNSE instruments API doesn't return a symbol.

    Returns list of tuples: (symbol, market_id, board_id, security_group_id, listing_date, final_trade_date)
    """
    if not symbols:
        return []
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (symbol, market_id)
                    symbol, market_id, board_id, security_group_id, listing_date, final_trade_date
                FROM security_definition
                WHERE symbol = ANY(%s)
                ORDER BY symbol, market_id, trading_date DESC
                """,
                (symbols,),
            )
            rows = cur.fetchall()
    logger.info(
        "[SECDEF] Fetched %d rows for %d fallback symbol(s)", len(rows), len(symbols)
    )
    return rows



def deactivate_instruments(symbols: list[str]) -> int:
    """
    Set is_active = false for all instruments whose symbol is in the given list.
    Used when a symbol disappears from security_definition (delisted / expired).

    Args:
        symbols: List of symbol strings to deactivate.

    Returns:
        Number of rows updated.
    """
    if not symbols:
        return 0

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE instrument_master
                SET is_active = false, last_synced_ts = clock_timestamp()
                WHERE symbol = ANY(%s) AND is_active = true
                """,
                (symbols,),
            )
            updated = cur.rowcount
        conn.commit()

    logger.info("[DEACTIVATE] Marked %d instruments as inactive: %s", updated, symbols)
    return updated


def reactivate_instruments(symbols: list[str]) -> int:
    """
    Set is_active = true for all instruments whose symbol is in the given list.
    Used when a previously deactivated symbol reappears in today's security_definition
    (e.g. a stock that was suspended/delisted and then relisted).

    Args:
        symbols: List of symbol strings to reactivate.

    Returns:
        Number of rows updated.
    """
    if not symbols:
        return 0

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE instrument_master
                SET is_active = true, last_synced_ts = clock_timestamp()
                WHERE symbol = ANY(%s) AND is_active = false
                """,
                (symbols,),
            )
            updated = cur.rowcount
        conn.commit()

    logger.info("[REACTIVATE] Reactivated %d instruments: %s", updated, symbols)
    return updated


# ── security_definition ───────────────────────────────────────────────────────

def count_secdef_today(today: "date_type") -> int:
    """Return number of security_definition rows for a given trading_date."""
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM security_definition WHERE trading_date = %s",
                (today,),
            )
            row = cur.fetchone()
    return row[0] if row else 0


def enrich_final_trade_date(
    symbols: list[str],
    trading_date: date_type,
) -> int:
    """
    Backfill final_trade_date in instrument_master from security_definition
    for symbols that currently have NULL in that column.

    Called after upsert_instrument_master when instruments come from the DNSE
    instruments API, which does not return final_trade_date. The secdef snapshot
    for today reliably contains this field for futures contracts.

    NULL is the EXPECTED value for non-futures instruments:
      - Equities (market_id IN ('STO','STX','UPX'))
      - ETFs, covered warrants (market_id = 'HCX')
      - Bonds
    Only futures contracts (market_id = 'DVX' or security_group_id = 'FU')
    carry a final_trade_date (contract expiry date). For all other instruments
    this function is a no-op because security_definition will not have
    final_trade_date set either.

    Args:
        symbols:      List of symbols to enrich (only NULLs will be updated).
        trading_date: The secdef trading_date to look up (typically today).

    Returns:
        Number of rows updated (0 is normal for non-DVX symbols).
    """
    if not symbols:
        return 0

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE instrument_master im
                SET    final_trade_date = sd.final_trade_date,
                       last_synced_ts   = clock_timestamp()
                FROM (
                    SELECT DISTINCT ON (symbol, market_id)
                        symbol,
                        market_id,
                        final_trade_date
                    FROM   security_definition
                    WHERE  symbol       = ANY(%s)
                      AND  trading_date = %s
                      AND  final_trade_date IS NOT NULL
                    ORDER  BY symbol, market_id, trading_date DESC
                ) sd
                WHERE  im.symbol    = sd.symbol
                  AND  im.market_id = sd.market_id
                  AND  im.final_trade_date IS NULL
                """,
                (symbols, trading_date),
            )
            updated = cur.rowcount
        conn.commit()

    if updated:
        logger.info("[ENRICH] Filled final_trade_date for %d instrument(s) from secdef", updated)
    else:
        logger.debug("[ENRICH] No NULL final_trade_date to fill for %d symbol(s)", len(symbols))
    return updated
