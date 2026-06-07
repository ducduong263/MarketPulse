"""
utils/instrument_delta.py — Business logic for dag_instrument_delta.

Logic:
  So sanh instrument_master (is_active=True) vs security_definition cua ngay hom nay.
  Day la nguon chan ly duy nhat — khong can so sanh 2 ngay.

Provides:
- detect_instrument_changes(today)    : diff instrument_master vs secdef today
- upsert_new_instruments(changes)     : fetch new symbols from DNSE API and upsert
                                        (fallback: upsert from secdef data if API misses any)
- deactivate_gone_instruments(changes): mark inactive symbols vs secdef today
- log_delta_summary(n_added, n_deactivated, changes): log summary of changes
"""

from __future__ import annotations

import logging
from datetime import date

logger = logging.getLogger(__name__)


def detect_instrument_changes(today: date) -> dict:
    """
    Compare active instrument_master symbols vs security_definition for today.

    Logic:
      - new_symbols  : in secdef today but NOT in instrument_master (any is_active)
      - to_deactivate: in instrument_master (is_active=True) but NOT in secdef today

    Returns a dict with:
      - today:          ISO date string
      - first_run:      True if instrument_master is empty
      - new_symbols:    symbols to fetch/upsert from API
      - to_deactivate:  symbols to set is_active=False
    """
    from utils.db import get_secdef_symbols, get_active_instrument_symbols, get_all_instrument_symbols

    # Minimum number of symbols expected in a valid secdef snapshot.
    # HOSE alone lists ~400 stocks; a full snapshot (HOSE+HNX+UPCOM+derivatives) has ~3000.
    # If we get less than this, the sync ran outside market hours or failed silently.
    MIN_SECDEF_COUNT = 200

    today_secdef    = get_secdef_symbols(today)
    all_instruments = get_all_instrument_symbols()    # all rows, regardless of is_active
    active_instruments = get_active_instrument_symbols()  # only is_active=True

    if not all_instruments:
        logger.info("[DELTA] instrument_master is empty — first run, will bulk-fetch all.")
        return {
            "today":          today.isoformat(),
            "first_run":      True,
            "new_symbols":    [],
            "to_deactivate":  [],
        }

    logger.info(
        "[DELTA] secdef today: %d | IM(active): %d | IM(all): %d",
        len(today_secdef), len(active_instruments), len(all_instruments),
    )

    # Safety guard: refuse to deactivate when secdef snapshot is incomplete
    if len(today_secdef) < MIN_SECDEF_COUNT:
        logger.warning(
            "[DELTA] SAFETY GUARD: today_secdef has only %d symbols (< %d minimum). "
            "secdef sync likely ran outside market hours or returned incomplete data. "
            "Skipping deactivation to prevent false mass-deactivation.",
            len(today_secdef), MIN_SECDEF_COUNT,
        )
        return {
            "today":         today.isoformat(),
            "first_run":     False,
            "new_symbols":   [],   # also skip new symbol fetch — data unreliable
            "to_deactivate": [],
        }

    # Symbols in secdef but completely absent from instrument_master (never seen before)
    new_symbols = sorted(today_secdef - all_instruments)

    # Active instruments that no longer appear in today's secdef
    to_deactivate = sorted(active_instruments - today_secdef)

    logger.info(
        "[DELTA] New (not in IM at all): %d | To deactivate (active but missing from secdef): %d",
        len(new_symbols), len(to_deactivate),
    )
    if new_symbols:
        logger.info("[DELTA] New symbols: %s", new_symbols)
    if to_deactivate:
        logger.info("[DELTA] To deactivate: %d symbols", len(to_deactivate))

    return {
        "today":          today.isoformat(),
        "first_run":      False,
        "new_symbols":    new_symbols,
        "to_deactivate":  to_deactivate,
    }


def upsert_new_instruments(changes: dict) -> int:
    """
    Fetch new instruments from DNSE API and upsert into instrument_master.

    Two paths:
      - first_run=True  : symbols=None -> paginated bulk fetch (~3000)
      - normal          : symbols=[...] -> 1 targeted API request

    Fallback: If the DNSE REST API does not return some requested symbols
    (e.g. brand-new derivatives/bonds not yet in the instruments endpoint),
    build minimal rows from security_definition data and upsert those too.
    """
    from utils.dnse_helpers import fetch_dnse_instruments, build_instrument_rows
    from utils.db import upsert_instrument_master, get_secdef_rows_for_symbols

    if changes.get("first_run"):
        logger.info("[UPSERT] First run — bulk paginated fetch")
        instruments = fetch_dnse_instruments()
        rows = build_instrument_rows(instruments)
        n = upsert_instrument_master(rows)
        logger.info("[UPSERT] Bulk upserted %d instruments", n)
        return n

    new_symbols = changes.get("new_symbols", [])
    if not new_symbols:
        logger.info("[UPSERT] No new symbols to fetch")
        return 0

    logger.info("[UPSERT] Fetching %d new symbol(s) from DNSE API", len(new_symbols))
    instruments = fetch_dnse_instruments(new_symbols)
    rows = build_instrument_rows(instruments)

    # Check if API missed any requested symbols
    returned_symbols = {r[0] for r in rows}  # r[0] = symbol
    missing = sorted(set(new_symbols) - returned_symbols)

    if missing:
        logger.warning(
            "[UPSERT] DNSE API did not return %d symbol(s): %s — "
            "falling back to security_definition data",
            len(missing), missing,
        )
        fallback_rows = _build_rows_from_secdef(missing, changes["today"])
        logger.info("[UPSERT] Built %d fallback row(s) from secdef", len(fallback_rows))
        rows.extend(fallback_rows)

    n = upsert_instrument_master(rows)
    logger.info("[UPSERT] Upserted %d instruments (%d from API, %d from secdef fallback)",
                n, len(returned_symbols & set(new_symbols)), len(missing))
    return n


def _build_rows_from_secdef(symbols: list[str], today_str: str) -> list[tuple]:
    """
    Build minimal instrument_master rows from security_definition data.
    Used as fallback when the DNSE REST API doesn't have a symbol yet.

    Returns rows compatible with upsert_instrument_master:
    (symbol, market_id, security_group_id, symbol_type,
     listed_date, short_name, full_name, index_name, is_active)
    """
    from utils.db import get_secdef_rows_for_symbols
    from datetime import date

    today = date.fromisoformat(today_str)
    secdef_rows = get_secdef_rows_for_symbols(symbols, today)

    rows = []
    seen: set[tuple] = set()
    for row in secdef_rows:
        # secdef row: (symbol, market_id, board_id, security_group_id, listing_date)
        symbol          = row[0]
        market_id       = row[1]
        # board_id       = row[2]  (not used in IM)
        security_grp    = row[3]
        listing_date    = row[4]  # may be None

        key = (symbol, market_id)
        if key in seen:
            continue
        seen.add(key)

        rows.append((
            symbol,
            market_id,
            security_grp,   # security_group_id
            None,           # symbol_type (unknown)
            listing_date,   # listed_date
            None,           # short_name (unknown)
            None,           # full_name (unknown)
            None,           # index_name (unknown)
            True,           # is_active
        ))

    return rows


def deactivate_gone_instruments(changes: dict) -> int:
    """
    Set is_active=False for all active instruments not present in today's secdef.
    This handles both newly delisted symbols AND long-stale historical symbols.
    """
    from utils.db import deactivate_instruments

    to_deactivate = changes.get("to_deactivate", [])
    if not to_deactivate:
        logger.info("[DEACTIVATE] No symbols to deactivate")
        return 0

    logger.info("[DEACTIVATE] Deactivating %d symbols not in today's secdef", len(to_deactivate))
    return deactivate_instruments(to_deactivate)


def log_delta_summary(n_added: int, n_deactivated: int, changes: dict) -> None:
    """Log a concise summary of what changed this run."""
    today    = changes.get("today", "?")
    new_syms = changes.get("new_symbols", [])
    gone     = changes.get("to_deactivate", [])

    logger.info("=" * 60)
    logger.info("[SUMMARY] dag_instrument_delta — %s vs secdef-today", today)
    logger.info("  New (not in IM):        %d symbols -> %d upserted",    len(new_syms), n_added)
    logger.info("  Deactivated (not in SD):%d symbols -> %d updated",     len(gone), n_deactivated)
    logger.info("=" * 60)

    if not new_syms and not gone:
        logger.info("[SUMMARY] No changes detected — instrument_master is up to date")
