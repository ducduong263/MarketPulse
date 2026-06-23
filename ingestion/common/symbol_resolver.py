"""
ingestion/common/symbol_resolver.py

Resolves the list of symbols to subscribe based on configurable filters.
Supports two modes:
  - db:     Query TimescaleDB using security_definition + instrument_master filters
  - static: Use WATCH_SYMBOLS + WATCH_DERIVATIVES from env (legacy fallback)

Always falls back to static mode if DB is unreachable.

--- Config source (priority order) ---

1. pipeline_config table (via ConfigStore — dynamic, polled every 60s)
2. Environment variables (fallback when DB is unavailable)

--- Config keys ---

symbol_filter_mode      : 'db' or 'static'  (default: 'static')

When mode=db, the following filters apply (all optional, AND-combined):

  symbol_filter_board_id  : board_id to filter on in secdef        (default: 'G1')
  symbol_filter_status    : comma-sep security_status values        (default: 'NO_HALT')
  symbol_filter_admin     : comma-sep admin_status values           (default: '')
  symbol_filter_sanction  : comma-sep trading_sanction_status       (default: 'NRM')
  symbol_filter_indexes   : comma-sep index_name values (OR logic)
                            e.g. 'VN30,VN100,HNX30' -> stocks in any of these indexes
  symbol_filter_groups    : comma-sep security_group_id (OR logic)
                            e.g. 'FU' -> always include all derivatives
  symbol_filter_market    : comma-sep market_id restriction (AND-filtered)
                            e.g. 'STO,DVX' — empty = all markets

When mode=db returns 0 symbols, falls back to static mode.

--- Hot-reload ---

resolve() reads current config from ConfigStore on every call.
Callers (e.g. producer_base) can call resolve() periodically to detect
symbol set changes without restarting the container.

--- Usage ---

    from ingestion.common.symbol_resolver import SymbolResolver

    resolver = SymbolResolver()
    symbols = resolver.resolve()                    # -> List[str]
    new_syms = resolver.resolve_new_symbols(current_set)  # -> set[str] of additions
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# ── DB connection ─────────────────────────────────────────────────────────────
_DB_PARAMS = dict(
    host    =lambda: os.getenv("postgres_host",     "localhost"),
    port    =lambda: int(os.getenv("postgres_port", "5432")),
    dbname  =lambda: os.getenv("postgres_db",       "market_data"),
    user    =lambda: os.getenv("postgres_user",     "marketpulse"),
    password=lambda: os.getenv("postgres_password", "mp_secret_2026"),
)


def _make_conn():
    import psycopg2
    return psycopg2.connect(
        host    =_DB_PARAMS["host"](),
        port    =_DB_PARAMS["port"](),
        dbname  =_DB_PARAMS["dbname"](),
        user    =_DB_PARAMS["user"](),
        password=_DB_PARAMS["password"](),
        connect_timeout=5,
    )


def _env_list(name: str, default: str = "") -> list[str]:
    """Read a comma-sep env var into a cleaned list. Returns [] if empty."""
    raw = os.getenv(name, default).strip()
    return [s.strip() for s in raw.split(",") if s.strip()] if raw else []


def _get_store():
    """Lazily import ConfigStore to avoid circular import at module level."""
    try:
        from ingestion.common.config_store import get_config_store
        return get_config_store()
    except Exception:
        return None


class SymbolResolver:
    """
    Resolves symbols to subscribe/request based on filter config.

    Config is read from ConfigStore (DB-backed, polled every 60s) on every
    resolve() call. Falls back to os.getenv() if ConfigStore is unavailable.

    Call .resolve() to get the final symbol list.
    Call .resolve_new_symbols(current_set) to get only symbols added since last check.
    """

    def __init__(self) -> None:
        # No state stored here — config is read fresh on every resolve() call
        pass

    # ── Config accessors ──────────────────────────────────────────────────────

    def _get_mode(self) -> str:
        store = _get_store()
        if store:
            return store.get("symbol_filter_mode", "").strip().lower() or \
                   os.getenv("SYMBOL_FILTER_MODE", "static").strip().lower()
        return os.getenv("SYMBOL_FILTER_MODE", "static").strip().lower()

    def _get_board_id(self) -> str:
        store = _get_store()
        if store:
            v = store.get("symbol_filter_board_id", "").strip()
            return v if v else os.getenv("SYMBOL_FILTER_BOARD_ID", "G1").strip() or "G1"
        return os.getenv("SYMBOL_FILTER_BOARD_ID", "G1").strip() or "G1"

    def _get_list_config(self, key: str, env_name: str, default: str = "") -> list[str]:
        store = _get_store()
        if store:
            lst = store.get_list(key, default=None)
            if lst is not None:
                return lst
        return _env_list(env_name, default)

    def _build_filter_config(self) -> dict:
        """Build current filter config dict from ConfigStore / env."""
        return {
            "mode":     self._get_mode(),
            "board_id": self._get_board_id(),
            "status":   self._get_list_config("symbol_filter_status",   "SYMBOL_FILTER_STATUS",   "NO_HALT"),
            "admin":    self._get_list_config("symbol_filter_admin",    "SYMBOL_FILTER_ADMIN",    ""),
            "sanction": self._get_list_config("symbol_filter_sanction", "SYMBOL_FILTER_SANCTION", "NRM"),
            "indexes":  self._get_list_config("symbol_filter_indexes",  "SYMBOL_FILTER_INDEXES",  ""),
            "groups":   self._get_list_config("symbol_filter_groups",   "SYMBOL_FILTER_GROUPS",   ""),
            "markets":  self._get_list_config("symbol_filter_market",   "SYMBOL_FILTER_MARKET",   ""),
        }

    # ── Public ────────────────────────────────────────────────────────────────

    def resolve(self) -> list[str]:
        """
        Return the resolved, sorted, deduplicated list of symbols.
        Reads current config from ConfigStore on every call.
        Tries DB mode first (if configured); falls back to static on error.
        """
        cfg = self._build_filter_config()

        if cfg["mode"] == "db":
            try:
                symbols = self._resolve_from_db(cfg)
                if symbols:
                    logger.info(
                        "[SymbolResolver] DB mode: resolved %d symbols "
                        "(board=%s status=%s admin=%s sanction=%s indexes=%s groups=%s markets=%s)",
                        len(symbols),
                        cfg["board_id"], cfg["status"], cfg["admin"], cfg["sanction"],
                        cfg["indexes"], cfg["groups"], cfg["markets"],
                    )
                    return symbols
                logger.warning(
                    "[SymbolResolver] DB query returned 0 symbols — falling back to static"
                )
            except Exception as e:
                logger.warning(
                    "[SymbolResolver] DB query failed (%s) — falling back to static", e
                )

        return self._resolve_static()

    def resolve_new_symbols(self, current_symbols: set[str]) -> set[str]:
        """
        Return symbols in the new resolved set that are NOT in current_symbols.
        Used by producers to detect hot-add of new symbols.

        Args:
            current_symbols: Set of symbols currently subscribed.

        Returns:
            Set of new symbols to subscribe (may be empty).
        """
        new_set = set(self.resolve())
        return new_set - current_symbols

    def describe(self) -> str:
        """Return a human-readable summary of current filter config."""
        cfg = self._build_filter_config()
        if cfg["mode"] == "db":
            parts = [f"mode=db board={cfg['board_id']}"]
            if cfg["status"]:   parts.append(f"status={','.join(cfg['status'])}")
            if cfg["admin"]:    parts.append(f"admin={','.join(cfg['admin'])}")
            if cfg["sanction"]: parts.append(f"sanction={','.join(cfg['sanction'])}")
            if cfg["indexes"]:  parts.append(f"indexes={','.join(cfg['indexes'])}")
            if cfg["groups"]:   parts.append(f"groups={','.join(cfg['groups'])}")
            if cfg["markets"]:  parts.append(f"markets={','.join(cfg['markets'])}")
            return " | ".join(parts)
        eq    = os.getenv("WATCH_SYMBOLS",     "ACB,FPT,VIC,SSI,HPG,MWG")
        deriv = os.getenv("WATCH_DERIVATIVES", "41I1G6000")
        return f"mode=static | WATCH_SYMBOLS={eq} | WATCH_DERIVATIVES={deriv}"

    # ── Private ───────────────────────────────────────────────────────────────

    def _resolve_static(self) -> list[str]:
        """Read WATCH_SYMBOLS + WATCH_DERIVATIVES from env."""
        eq    = _env_list("WATCH_SYMBOLS",     "ACB,FPT,VIC,SSI,HPG,MWG")
        deriv = _env_list("WATCH_DERIVATIVES", "41I1G6000")
        symbols = sorted(set(eq + deriv))
        logger.info("[SymbolResolver] Static mode: %d symbols", len(symbols))
        return symbols

    def _resolve_from_db(self, cfg: dict) -> list[str]:
        """
        Build and run the filter query against TimescaleDB.

        Logic:
          Required (all symbols must satisfy):
            - sd.trading_date = CURRENT_DATE (ICT)
            - sd.board_id     = symbol_filter_board_id
            - sd.security_status IN (symbol_filter_status)
            - sd.admin_status IN (symbol_filter_admin)
            - sd.trading_sanction_status IN (symbol_filter_sanction)
            - im.is_active = true

          Optional market filter (if symbol_filter_market set):
            - sd.market_id IN (symbol_filter_market)

          Symbol selection filter (if EITHER is set, uses OR):
            - im.security_group_id IN (symbol_filter_groups)
            - im.index_name contains any of (symbol_filter_indexes)

          If NEITHER groups NOR indexes is set: include all symbols passing base filters.
        """
        from datetime import datetime, timezone, timedelta

        # Today in ICT (UTC+7)
        today = (datetime.now(timezone.utc) + timedelta(hours=7)).date()

        # Build WHERE clauses + params
        where: list[str] = []
        params: list = []

        # -- Required filters --
        where.append("sd.trading_date = %s")
        params.append(today)

        where.append("sd.board_id = %s")
        params.append(cfg["board_id"])

        if cfg["status"]:
            where.append("sd.security_status = ANY(%s)")
            params.append(cfg["status"])

        if cfg["admin"]:
            where.append("sd.admin_status = ANY(%s)")
            params.append(cfg["admin"])

        if cfg["sanction"]:
            where.append("sd.trading_sanction_status = ANY(%s)")
            params.append(cfg["sanction"])

        where.append("im.is_active = true")

        # -- Optional market filter --
        if cfg["markets"]:
            where.append("sd.market_id = ANY(%s)")
            params.append(cfg["markets"])

        # -- Symbol selection: groups OR indexes (OR combined) --
        selection_clauses: list[str] = []

        if cfg["groups"]:
            selection_clauses.append("im.security_group_id = ANY(%s)")
            params.append(cfg["groups"])

        if cfg["indexes"]:
            index_pattern = "|".join(
                f"(^|,){idx}(,|$)" for idx in cfg["indexes"]
            )
            selection_clauses.append("im.index_name ~ %s")
            params.append(index_pattern)

        if selection_clauses:
            where.append(f"({' OR '.join(selection_clauses)})")

        sql = f"""
            SELECT DISTINCT sd.symbol
            FROM security_definition sd
            JOIN instrument_master im
              ON sd.symbol = im.symbol
             AND sd.market_id = im.market_id
            WHERE {' AND '.join(where)}
            ORDER BY sd.symbol
        """

        logger.debug("[SymbolResolver] SQL:\n%s\nparams=%s", sql, params)

        conn = _make_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        finally:
            conn.close()

        return [r[0] for r in rows]
