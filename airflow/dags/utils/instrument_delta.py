"""
utils/instrument_delta.py — Business logic for dag_instrument_delta.

Logic:
  Compare instrument_master vs today's security_definition.
  Deactivation scope: only applies to STO (HOSE), STX (HNX), DVX (Derivatives) markets.
  UPCOM (UPX) and HCX are excluded from deactivation because their data arrives late (REST API ~09:15 ICT).

Provides:
- detect_instrument_changes(today)         : diff instrument_master vs secdef today
- upsert_new_instruments(changes)          : fetch new symbols from DNSE API and upsert
                                             (fallback: upsert from secdef data if API misses any)
- reactivate_returned_instruments(changes) : reactivate symbols that were inactive and reappear
- deactivate_gone_instruments(changes)     : mark inactive symbols vs secdef today
                                             (only for STO/STX/DVX)
- refresh_dvx_metadata()                   : re-fetch ALL active DVX symbols from DNSE API
                                             to capture symbol_type / short_name changes
                                             after a futures roll (secdef does not track this)
- log_delta_summary(...)                   : log summary of changes
"""

from __future__ import annotations

import logging
from datetime import date

logger = logging.getLogger(__name__)

# Only apply deactivation to these markets.
# UPCOM (UPX) and HCX are excluded because their data arrives late (~09:15 ICT via REST API)
# and should not be falsely deactivated in the morning.
DEACTIVATION_MARKETS = frozenset({"STO", "STX", "DVX"})

def detect_instrument_changes(today: date) -> dict:
    """
    Compare instrument_master symbols vs security_definition for today.

    Logic:
      - new_symbols   : in secdef today but NOT in instrument_master (any is_active)
      - to_reactivate : in secdef today AND in instrument_master with is_active=False
                        (symbol disappeared previously, now relisted/unsuspended)
      - to_deactivate : in instrument_master (is_active=True, market in STO/STX/DVX)
                        but NOT in today's secdef.
                        UPCOM (UPX) and HCX are excluded from deactivation because of late arrival.

    Returns a dict with:
      - today:          ISO date string
      - first_run:      True if instrument_master is empty
      - new_symbols:    symbols to fetch/upsert from API
      - to_reactivate:  symbols to set is_active=True (returned after deactivation)
      - to_deactivate:  symbols to set is_active=False (STO/STX/DVX only)
    """
    from utils.db import (
        get_secdef_symbols,
        get_active_instrument_symbols,
        get_active_instrument_symbols_by_markets,
        get_all_instrument_symbols,
        get_inactive_instrument_symbols,
        get_symbols_missing_from_instrument_master,
    )

    # Minimum number of symbols expected in a valid secdef snapshot.
    # STO+STX+DVX alone = ~1,871 symbols. If we get less, WSS likely failed.
    MIN_SECDEF_COUNT = 200

    today_secdef         = get_secdef_symbols(today)
    all_instruments      = get_all_instrument_symbols()     # all rows, regardless of is_active
    active_instruments   = get_active_instrument_symbols()  # only is_active=True
    inactive_instruments = get_inactive_instrument_symbols()  # only is_active=False

    if not all_instruments:
        logger.info("[DELTA] instrument_master is empty -- first run, will bulk-fetch all.")
        return {
            "today":          today.isoformat(),
            "first_run":      True,
            "new_symbols":    [],
            "to_reactivate":  [],
            "to_deactivate":  [],
        }

    logger.info(
        "[DELTA] secdef today: %d | IM(active): %d | IM(inactive): %d | IM(all): %d",
        len(today_secdef), len(active_instruments), len(inactive_instruments), len(all_instruments),
    )

    # Symbols in secdef today but completely absent from instrument_master (never seen before)
    new_symbols_today = today_secdef - all_instruments

    # Also find any symbols ever present in security_definition but missing from IM
    # (handles websocket sync additions from later in the day/yesterday)
    missing_from_im = get_symbols_missing_from_instrument_master()
    new_symbols = sorted(new_symbols_today | missing_from_im)

    # Symbols that were previously deactivated and now reappear in secdef
    to_reactivate = sorted(today_secdef & inactive_instruments)

    # Safety guard: refuse to deactivate when secdef snapshot is incomplete
    if len(today_secdef) < MIN_SECDEF_COUNT:
        logger.warning(
            "[DELTA] SAFETY GUARD: today_secdef has only %d symbols (< %d minimum). "
            "secdef sync likely ran outside market hours or returned incomplete data. "
            "Skipping deactivation to prevent false mass-deactivation.",
            len(today_secdef), MIN_SECDEF_COUNT,
        )
        to_deactivate = []
    else:
        # Only deactivate symbols belonging to STO/STX/DVX that no longer appear in today's secdef.
        # UPCOM (UPX) and HCX are excluded because their data arrives late via REST API (~09:15 ICT).
        active_priority = get_active_instrument_symbols_by_markets(DEACTIVATION_MARKETS)
        to_deactivate = sorted(active_priority - today_secdef)
        logger.info(
            "[DELTA] Deactivation scope: %s only | Active in scope: %d | Missing from secdef: %d",
            "/".join(sorted(DEACTIVATION_MARKETS)), len(active_priority), len(to_deactivate),
        )

    logger.info(
        "[DELTA] New (not in IM at all): %d | To reactivate (inactive but back in secdef): %d | To deactivate (active but missing from secdef): %d",
        len(new_symbols), len(to_reactivate), len(to_deactivate),
    )
    if new_symbols:
        logger.info("[DELTA] New symbols: %s", new_symbols)
    if to_reactivate:
        logger.info("[DELTA] To reactivate: %s", to_reactivate)
    if to_deactivate:
        logger.info("[DELTA] To deactivate: %d symbols", len(to_deactivate))

    return {
        "today":          today.isoformat(),
        "first_run":      False,
        "new_symbols":    new_symbols,
        "to_reactivate":  to_reactivate,
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

    After upsert, enrich_final_trade_date() is called to backfill the
    final_trade_date column for any instruments that have NULL there,
    since the DNSE instruments endpoint does not return expiry dates.
    """
    from utils.dnse_helpers import fetch_dnse_instruments, build_instrument_rows
    from utils.db import upsert_instrument_master, get_secdef_rows_for_symbols, enrich_final_trade_date
    from datetime import date

    today = date.fromisoformat(changes["today"])

    if changes.get("first_run"):
        logger.info("[UPSERT] First run -- bulk paginated fetch")
        instruments = fetch_dnse_instruments()
        rows = build_instrument_rows(instruments)
        n = upsert_instrument_master(rows)
        # Enrich final_trade_date for all symbols just upserted (DNSE API has no expiry data)
        all_symbols = [r[0] for r in rows]
        enrich_final_trade_date(all_symbols, today)
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
            "[UPSERT] DNSE API did not return %d symbol(s): %s -- "
            "falling back to security_definition data",
            len(missing), missing,
        )
        fallback_rows = _build_rows_from_secdef(missing, changes["today"])
        logger.info("[UPSERT] Built %d fallback row(s) from secdef", len(fallback_rows))
        rows.extend(fallback_rows)

    n = upsert_instrument_master(rows)
    # Enrich final_trade_date for DNSE-sourced symbols (fallback rows already have it from secdef)
    enrich_final_trade_date(sorted(returned_symbols & set(new_symbols)), today)
    logger.info("[UPSERT] Upserted %d instruments (%d from API, %d from secdef fallback)",
                n, len(returned_symbols & set(new_symbols)), len(missing))
    return n


def _build_rows_from_secdef(symbols: list[str], today_str: str) -> list[tuple]:
    """
    Build minimal instrument_master rows from security_definition data.
    Used as fallback when the DNSE REST API doesn't have a symbol yet.

    Returns rows compatible with upsert_instrument_master:
    (symbol, market_id, security_group_id, symbol_type,
     listed_date, final_trade_date, short_name, full_name, index_name, is_active)
    """
    from utils.db import get_secdef_rows_for_symbols

    secdef_rows = get_secdef_rows_for_symbols(symbols)

    rows = []
    seen: set[tuple] = set()
    for row in secdef_rows:
        # secdef row: (symbol, market_id, board_id, security_group_id, listing_date, final_trade_date)
        symbol           = row[0]
        market_id        = row[1]
        # board_id        = row[2]  (not used in IM)
        security_grp     = row[3]
        listing_date     = row[4]  # may be None
        final_trade_date = row[5]  # may be None (only futures have this)

        key = (symbol, market_id)
        if key in seen:
            continue
        seen.add(key)

        rows.append((
            symbol,
            market_id,
            security_grp,     # security_group_id
            None,             # symbol_type (unknown from secdef)
            listing_date,     # listed_date
            final_trade_date, # final_trade_date (populated for futures from secdef)
            None,             # short_name (unknown)
            None,             # full_name (unknown)
            None,             # index_name (unknown)
            True,             # is_active
        ))

    return rows


def reactivate_returned_instruments(changes: dict) -> int:
    """
    Set is_active=True for symbols that were previously deactivated but have
    reappeared in today's security_definition (e.g. relisted / suspension lifted).
    """
    from utils.db import reactivate_instruments

    to_reactivate = changes.get("to_reactivate", [])
    if not to_reactivate:
        logger.info("[REACTIVATE] No symbols to reactivate")
        return 0

    logger.info("[REACTIVATE] Reactivating %d symbols back in today's secdef: %s", len(to_reactivate), to_reactivate)
    return reactivate_instruments(to_reactivate)


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


def refresh_dvx_metadata() -> int:
    """
    Re-fetch ALL currently active DVX (derivatives) symbols from the DNSE instruments
    API and upsert them into instrument_master.

    Why this is necessary
    ---------------------
    The DNSE API returns a dynamic 'symbolType' field (e.g. VN30F1M, VN30F2M) that
    reflects the CURRENT rolling position of a futures contract in the term structure.
    When a front-month contract expires, the next-month contract's symbolType changes
    (e.g. 41I1G7000 rolls from VN30F2M -> VN30F1M), and its short_name changes too.
    This information is NOT stored in security_definition, so the only way to detect
    and persist the change is to re-query the DNSE instruments API every day.

    Strategy: Option A (always refresh)
    ------------------------------------
    DVX typically has ~20 active contracts. The cost of re-fetching all of them in a
    single API call each day is negligible compared to the benefit of always having
    up-to-date symbol_type and short_name for every futures contract.

    The upsert uses COALESCE for final_trade_date so existing expiry dates are never
    overwritten by NULL values from the DNSE instruments endpoint.

    Returns:
        Number of rows upserted.
    """
    from utils.db import get_active_instrument_symbols_by_market, upsert_instrument_master
    from utils.dnse_helpers import fetch_dnse_instruments, build_instrument_rows

    active_dvx = get_active_instrument_symbols_by_market("DVX")
    if not active_dvx:
        logger.info("[DVX_REFRESH] No active DVX symbols found — skipping")
        return 0

    symbols = sorted(active_dvx)
    logger.info("[DVX_REFRESH] Re-fetching metadata for %d active DVX symbols: %s", len(symbols), symbols)

    instruments = fetch_dnse_instruments(symbols)
    if not instruments:
        logger.warning("[DVX_REFRESH] DNSE API returned no instruments for DVX symbols")
        return 0

    rows = build_instrument_rows(instruments)
    n = upsert_instrument_master(rows)
    logger.info("[DVX_REFRESH] Refreshed %d DVX instrument rows", n)
    return n


def log_delta_summary(
    n_added: int, n_reactivated: int, n_deactivated: int,
    n_dvx_refreshed: int, changes: dict,
) -> None:
    """Log a concise summary of what changed this run."""
    today         = changes.get("today", "?")
    new_syms      = changes.get("new_symbols", [])
    reactivated   = changes.get("to_reactivate", [])
    gone          = changes.get("to_deactivate", [])

    logger.info("=" * 60)
    logger.info("[SUMMARY] dag_instrument_delta -- %s vs secdef-today", today)
    logger.info("  New (not in IM):        %d symbols -> %d upserted",   len(new_syms), n_added)
    logger.info("  Reactivated (returned): %d symbols -> %d updated",    len(reactivated), n_reactivated)
    logger.info("  Deactivated (not in SD):%d symbols -> %d updated",    len(gone), n_deactivated)
    logger.info("  DVX metadata refreshed: %d symbols (symbol_type/short_name)", n_dvx_refreshed)
    logger.info("=" * 60)

    if not new_syms and not reactivated and not gone:
        logger.info("[SUMMARY] No structural changes -- DVX metadata refresh ran as scheduled")

