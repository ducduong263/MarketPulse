"""
Trading calendar utilities.

is_trading_day(): queries trading_calendar table.
  - The table contains only rows for actual trading days.
  - A date is a trading day if it appears in the table.

is_trading_hours(): checks if current ICT time is within trading session.
  Trading hours: 09:00–15:00 ICT (UTC+7), Monday–Friday.
"""

import logging
import os
from datetime import datetime, timezone, timedelta

import psycopg2

logger = logging.getLogger(__name__)

_DB_CONFIG = {
    "host":     os.getenv("postgres_host", "timescaledb"),
    "port":     int(os.getenv("postgres_port", "5432")),
    "dbname":   os.getenv("postgres_db", "market_data"),
    "user":     os.getenv("postgres_user", "marketpulse"),
    "password": os.getenv("postgres_password", ""),
}

ICT = timezone(timedelta(hours=7))

# Trading sessions (ICT)
_AM_START = (9, 0)
_AM_END   = (11, 30)
_PM_START = (13, 0)
_PM_END   = (14, 45)


def now_ict() -> datetime:
    return datetime.now(ICT)


def is_trading_hours() -> bool:
    """True if current ICT time is within morning or afternoon session, weekday."""
    now = now_ict()
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    
    am_start = now.replace(hour=_AM_START[0], minute=_AM_START[1], second=0, microsecond=0)
    am_end   = now.replace(hour=_AM_END[0],   minute=_AM_END[1],   second=0, microsecond=0)
    pm_start = now.replace(hour=_PM_START[0], minute=_PM_START[1], second=0, microsecond=0)
    pm_end   = now.replace(hour=_PM_END[0],   minute=_PM_END[1],   second=0, microsecond=0)

    return (am_start <= now <= am_end) or (pm_start <= now <= pm_end)


def get_current_session_start() -> datetime:
    """
    Returns the start timestamp of the active session (morning or afternoon).
    Used to clamp gap calculations so we don't alert on overnight or lunch gaps.
    """
    now = now_ict()
    pm_start = now.replace(hour=_PM_START[0], minute=_PM_START[1], second=0, microsecond=0)
    
    # If we are in the PM session, return PM start, otherwise return AM start
    if now >= pm_start:
        return pm_start
    
    return now.replace(hour=_AM_START[0], minute=_AM_START[1], second=0, microsecond=0)


def is_trading_day(date: datetime | None = None) -> bool:
    """
    True if the given date (default: today ICT) is in the trading_calendar table.
    Falls back to True on DB error to avoid silencing alerts due to a DB blip.
    """
    check_date = (date or now_ict()).date()
    try:
        conn = psycopg2.connect(**_DB_CONFIG)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM trading_calendar WHERE trading_date = %s LIMIT 1",
                    (check_date,),
                )
                return cur.fetchone() is not None
        finally:
            conn.close()
    except Exception as exc:
        # On DB failure: conservatively assume it IS a trading day
        # so alerts are not silenced if the DB is having trouble.
        logger.warning(
            "trading_calendar query failed, assuming trading day: %s", exc
        )
        return True


def should_monitor() -> bool:
    """Combined gate: only run data checks during active trading hours on trading days."""
    return is_trading_hours() and is_trading_day()


_MONITOR_START = (8, 30)
_MONITOR_END   = (15, 15)


def is_monitoring_hours() -> bool:
    """True if current ICT time is within broad monitoring hours (08:30–15:15)."""
    now = now_ict()
    if now.weekday() >= 5:  # Skip weekend
        return False
    monitor_start = now.replace(hour=_MONITOR_START[0], minute=_MONITOR_START[1], second=0, microsecond=0)
    monitor_end   = now.replace(hour=_MONITOR_END[0],   minute=_MONITOR_END[1],   second=0, microsecond=0)
    return monitor_start <= now <= monitor_end


def should_monitor_infra() -> bool:
    """True if we should monitor container health (08:30–15:15 on trading days)."""
    return is_monitoring_hours() and is_trading_day()
