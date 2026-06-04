"""
ingestion/common/symbol_resolver.py

Resolves the list of symbols to subscribe based on configurable filters.
Supports two modes:
  - db:     Query TimescaleDB using security_definition + instrument_master filters
  - static: Use WATCH_SYMBOLS + WATCH_DERIVATIVES from env (legacy fallback)

Always falls back to static mode if DB is unreachable.

--- Env vars ---

SYMBOL_FILTER_MODE      : 'db' or 'static'  (default: 'static')

When MODE=db, the following filters apply (all optional, AND-combined):

  SYMBOL_FILTER_BOARD_ID  : board_id to filter on in secdef
                            default: 'G1'
  SYMBOL_FILTER_STATUS    : comma-sep security_status values
                            default: 'NO_HALT'
  SYMBOL_FILTER_ADMIN     : comma-sep admin_status values
                            default: 'NRM'
  SYMBOL_FILTER_SANCTION  : comma-sep trading_sanction_status values
                            default: 'NRM'
  SYMBOL_FILTER_INDEXES   : comma-sep index_name values to include (OR logic)
                            e.g. 'VN30,VN100'  ->  stocks in VN30 or VN100
                            Leave empty to skip index filtering
  SYMBOL_FILTER_GROUPS    : comma-sep security_group_id to always include (OR logic)
                            e.g. 'FU' -> always include all derivatives
                            Leave empty to skip group filtering
  SYMBOL_FILTER_MARKET    : comma-sep market_id to restrict (AND-filtered)
                            e.g. 'STO,DVX'
                            Leave empty for all markets

When MODE=db is set but DB query returns nothing, falls back to static.

Static fallback:
  WATCH_SYMBOLS           : comma-sep equity symbols  (default: ACB,FPT,VIC,SSI,HPG,MWG)
  WATCH_DERIVATIVES       : comma-sep derivative codes (default: 41I1G6000)

--- Usage ---

    from ingestion.common.symbol_resolver import SymbolResolver

    resolver = SymbolResolver()
    symbols = resolver.resolve()  # -> List[str]
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# ── DB connection ────────────────
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


class SymbolResolver:
    """
    Resolves symbols to subscribe/request based on env-driven filter config.

    Call .resolve() to get the final symbol list.
    """

    def __init__(self) -> None:
        self.mode      = os.getenv("SYMBOL_FILTER_MODE", "static").strip().lower()
        self.board_id  = os.getenv("SYMBOL_FILTER_BOARD_ID",  "G1").strip() or "G1"
        self.status    = _env_list("SYMBOL_FILTER_STATUS",   "NO_HALT")
        self.admin     = _env_list("SYMBOL_FILTER_ADMIN",    "")
        self.sanction  = _env_list("SYMBOL_FILTER_SANCTION", "NRM")
        self.indexes   = _env_list("SYMBOL_FILTER_INDEXES",  "")   # e.g. ["VN30","VN100"]
        self.groups    = _env_list("SYMBOL_FILTER_GROUPS",   "")   # e.g. ["FU"]
        self.markets   = _env_list("SYMBOL_FILTER_MARKET",   "")   # e.g. ["STO","DVX"]

    # ── Public ────────────────────────────────────────────────────────────    ────

    def resolve(self) -> list[str]:
        """
        Return the resolved, sorted, deduplicated list of symbols.

        Tries DB mode first (if configured); falls back to static on error.
        """
        if self.mode == "db":
            try:
                symbols = self._resolve_from_db()
                if symbols:
                    logger.info(
                        "[SymbolResolver] DB mode: resolved %d symbols "
                        "(board=%s status=%s admin=%s sanction=%s indexes=%s groups=%s markets=%s)",
                        len(symbols),
                        self.board_id, self.status, self.admin, self.sanction,
                        self.indexes, self.groups, self.markets,
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

    def describe(self) -> str:
        """Return a human-readable summary of current filter config."""
        if self.mode == "db":
            parts = [f"mode=db board={self.board_id}"]
            if self.status:   parts.append(f"status={','.join(self.status)}")
            if self.admin:    parts.append(f"admin={','.join(self.admin)}")
            if self.sanction: parts.append(f"sanction={','.join(self.sanction)}")
            if self.indexes:  parts.append(f"indexes={','.join(self.indexes)}")
            if self.groups:   parts.append(f"groups={','.join(self.groups)}")
            if self.markets:  parts.append(f"markets={','.join(self.markets)}")
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

    def _resolve_from_db(self) -> list[str]:
        """
        Build and run the filter query against TimescaleDB.

        Logic:
          Required (all symbols must satisfy):
            - sd.trading_date = CURRENT_DATE (ICT)
            - sd.board_id     = SYMBOL_FILTER_BOARD_ID
            - sd.security_status IN (SYMBOL_FILTER_STATUS)
            - sd.admin_status IN (SYMBOL_FILTER_ADMIN)
            - sd.trading_sanction_status IN (SYMBOL_FILTER_SANCTION)
            - im.is_active = true

          Optional market filter (if SYMBOL_FILTER_MARKET set):
            - sd.market_id IN (SYMBOL_FILTER_MARKET)

          Symbol selection filter (if EITHER is set, uses OR):
            - im.security_group_id IN (SYMBOL_FILTER_GROUPS)   -- e.g. FU (derivatives)
            - im.index_name contains any of (SYMBOL_FILTER_INDEXES) -- e.g. VN30, VN100

          If NEITHER groups NOR indexes is set: include all symbols passing the base filters.
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
        params.append(self.board_id)

        if self.status:
            where.append("sd.security_status = ANY(%s)")
            params.append(self.status)

        if self.admin:
            where.append("sd.admin_status = ANY(%s)")
            params.append(self.admin)

        if self.sanction:
            where.append("sd.trading_sanction_status = ANY(%s)")
            params.append(self.sanction)

        where.append("im.is_active = true")

        # -- Optional market filter --
        if self.markets:
            where.append("sd.market_id = ANY(%s)")
            params.append(self.markets)

        # -- Symbol selection: groups OR indexes (OR combined) --
        # If neither configured, all symbols passing base filters are included.
        selection_clauses: list[str] = []

        if self.groups:
            selection_clauses.append("im.security_group_id = ANY(%s)")
            params.append(self.groups)

        if self.indexes:
            index_pattern = "|".join(
                f"(^|,){idx}(,|$)" for idx in self.indexes
            )  # e.g. "(^|,)VN30(,|$)|(^|,)VN100(,|$)"
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
