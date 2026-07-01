"""
Data freshness check — queries MAX(exchange_ts) from market tables.

Uses exchange_ts (timestamp from the exchange) rather than ingested_ts,
so a producer drop or broker gap is correctly detected even if the
consumer process itself is still running.

Thresholds (configurable via env):
  - market_trade:   TRADE_GAP_MINUTES  (default: 3)
  - order_book_l2:  QUOTE_GAP_MINUTES  (default: 5)
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

import psycopg2

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
_DB_CONFIG = {
    "host":     os.getenv("postgres_host", "timescaledb"),
    "port":     int(os.getenv("postgres_port", "5432")),
    "dbname":   os.getenv("postgres_db", "market_data"),
    "user":     os.getenv("postgres_user", "marketpulse"),
    "password": os.getenv("postgres_password", ""),
}

_TABLES: dict[str, int] = {
    "market_trade":   int(os.getenv("TRADE_GAP_MINUTES", "3")),
    "order_book_l2":  int(os.getenv("QUOTE_GAP_MINUTES", "5")),
}


@dataclass
class FreshnessResult:
    table: str
    last_exchange_ts: datetime | None
    gap_minutes: float | None
    threshold_minutes: int
    is_stale: bool


def check() -> list[FreshnessResult]:
    """
    Returns FreshnessResult for each monitored table.
    Raises on DB connection failure (caller handles escalation).
    """
    results: list[FreshnessResult] = []
    now_utc = datetime.now(timezone.utc)

    conn = psycopg2.connect(**_DB_CONFIG)
    try:
        with conn.cursor() as cur:
            for table, threshold in _TABLES.items():
                cur.execute(
                    f"SELECT MAX(exchange_ts) FROM {table}"  # noqa: S608
                )
                row = cur.fetchone()
                last_ts: datetime | None = row[0] if row else None

                if last_ts is None:
                    # Table empty — treat as stale
                    results.append(FreshnessResult(
                        table=table,
                        last_exchange_ts=None,
                        gap_minutes=None,
                        threshold_minutes=threshold,
                        is_stale=True,
                    ))
                    continue

                # Ensure timezone-aware
                if last_ts.tzinfo is None:
                    last_ts = last_ts.replace(tzinfo=timezone.utc)

                # Clamp to the current session start (morning or afternoon open)
                # to prevent alert spikes during lunch break or at market open.
                from ingestion.monitor.checks.trading_hours import get_current_session_start
                session_start_ict = get_current_session_start()
                session_start_utc = session_start_ict.astimezone(timezone.utc)

                effective_ts = max(last_ts, session_start_utc)

                gap = (now_utc - effective_ts).total_seconds() / 60
                is_stale = gap > threshold

                # skip in ATC and ATO 
                if table == "market_trade":
                    from ingestion.monitor.checks.trading_hours import now_ict
                    t = now_ict().time()
                    if (t.hour == 9 and t.minute < 15) or (t.hour == 14 and 30 <= t.minute < 45):
                        is_stale = False

                results.append(FreshnessResult(
                    table=table,
                    last_exchange_ts=last_ts,
                    gap_minutes=round((now_utc - last_ts).total_seconds() / 60, 1),
                    threshold_minutes=threshold,
                    is_stale=is_stale,
                ))
    finally:
        conn.close()

    return results


def format_result(r: FreshnessResult) -> str:
    """Format single result as a Telegram bullet line."""
    if r.last_exchange_ts is None:
        return f"📋 `{r.table}` — Chưa có dữ liệu"
    if r.is_stale:
        return f"📋 `{r.table}` — {r.gap_minutes} phút không có dữ liệu mới"
    return f"📋 `{r.table}` — OK ({r.gap_minutes} phút trước)"
