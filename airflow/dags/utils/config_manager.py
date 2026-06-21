"""
airflow/dags/utils/config_manager.py

Database helpers for managing pipeline_config table.

Provides:
  - get_all_configs()       : get all config key-value pairs
  - get_config(key)         : get a single config value
  - set_config(key, value)  : upsert a config key-value pair
  - set_configs(updates)    : upsert multiple key-value pairs at once
  - reset_to_defaults()     : reset all config to default values (from .env)
  - list_config_history()   : show updated_at timestamps per key
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


# ── Default values — mirrors what's in 05_pipeline_config.sql seed ───────────
_DEFAULTS: dict[str, tuple[str, str, str]] = {
    # key: (value, group_name, description)
    "symbol_filter_mode":     ("db",              "symbol_filter", "Resolver mode: db or static"),
    "symbol_filter_indexes":  ("VN30,VN100,HNX30","symbol_filter", "Comma-sep index names (OR logic)"),
    "symbol_filter_groups":   ("FU",              "symbol_filter", "Comma-sep security_group_id to always include"),
    "symbol_filter_status":   ("NO_HALT",         "symbol_filter", "Comma-sep security_status values"),
    "symbol_filter_admin":    ("NRM",             "symbol_filter", "Comma-sep admin_status values. Empty = no filter"),
    "symbol_filter_sanction": ("NRM",             "symbol_filter", "Comma-sep trading_sanction_status"),
    "symbol_filter_board_id": ("G1",              "symbol_filter", "board_id filter"),
    "symbol_filter_market":   ("",                "symbol_filter", "Comma-sep market_id restriction. Empty = all markets"),
    "flush_batch_size":       ("100",             "flush",         "Consumer batch size before forced flush"),
    "flush_timeout_seconds":  ("2.0",             "flush",         "Consumer batch timeout (seconds)"),
    "stats_flush_interval":   ("30",              "flush",         "StatsReporter flush interval (seconds)"),
    "connection_timeout":     ("5",               "connection",    "DB connect_timeout for config polling (seconds)"),
}

_UPSERT_SQL = """
    INSERT INTO pipeline_config (key, value, group_name, description, updated_at)
    VALUES (%s, %s, %s, %s, NOW())
    ON CONFLICT (key) DO UPDATE SET
        value      = EXCLUDED.value,
        updated_at = NOW()
"""


def get_all_configs() -> dict[str, dict]:
    """
    Return all pipeline_config rows as dict:
    { key: { value, group_name, updated_at, description } }
    """
    from utils.db import get_db_conn
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT key, value, group_name, updated_at, description "
                "FROM pipeline_config ORDER BY group_name, key"
            )
            rows = cur.fetchall()

    result = {}
    for key, value, group_name, updated_at, description in rows:
        result[key] = {
            "value":       value,
            "group_name":  group_name,
            "updated_at":  updated_at.isoformat() if updated_at else None,
            "description": description,
        }
    return result


def get_config(key: str) -> str | None:
    """Return value for a single config key, or None if not found."""
    from utils.db import get_db_conn
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM pipeline_config WHERE key = %s", (key,))
            row = cur.fetchone()
    return row[0] if row else None


def set_config(key: str, value: str) -> None:
    """
    Upsert a single config key-value.
    Preserves group_name and description from existing row (or defaults).
    """
    from utils.db import get_db_conn

    defaults = _DEFAULTS.get(key, (value, "default", ""))
    _, group_name, description = defaults

    # Keep existing group_name/description if row exists
    existing = _get_meta(key)
    if existing:
        group_name, description = existing

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_UPSERT_SQL, (key, value, group_name, description))
        conn.commit()

    logger.info("[ConfigManager] Set '%s' = '%s' (group=%s)", key, value, group_name)


def set_configs(updates: dict[str, str]) -> int:
    """
    Upsert multiple key-value pairs at once.

    Args:
        updates: dict of { key: value }

    Returns:
        Number of keys updated.
    """
    from utils.db import get_db_conn

    rows = []
    for key, value in updates.items():
        defaults = _DEFAULTS.get(key, (value, "default", ""))
        _, group_name, description = defaults
        existing = _get_meta(key)
        if existing:
            group_name, description = existing
        rows.append((key, value, group_name, description))

    if not rows:
        return 0

    import psycopg2.extras
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO pipeline_config (key, value, group_name, description)
                VALUES %s
                ON CONFLICT (key) DO UPDATE SET
                    value      = EXCLUDED.value,
                    updated_at = NOW()
                """,
                [(k, v, g, d) for k, v, g, d in rows],
            )
        conn.commit()

    logger.info("[ConfigManager] Batch upsert: %d keys", len(rows))
    return len(rows)


def reset_to_defaults() -> int:
    """
    Reset all known config keys to their default values.
    Uses ON CONFLICT DO UPDATE — always overwrites.

    Returns:
        Number of keys reset.
    """
    import psycopg2.extras
    from utils.db import get_db_conn

    rows = [
        (key, val, group, desc)
        for key, (val, group, desc) in _DEFAULTS.items()
    ]

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO pipeline_config (key, value, group_name, description)
                VALUES %s
                ON CONFLICT (key) DO UPDATE SET
                    value      = EXCLUDED.value,
                    description = EXCLUDED.description,
                    updated_at = NOW()
                """,
                rows,
            )
        conn.commit()

    logger.info("[ConfigManager] Reset %d config keys to defaults", len(rows))
    return len(rows)


def _get_meta(key: str) -> tuple[str, str] | None:
    """Return (group_name, description) for existing key, or None."""
    from utils.db import get_db_conn
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT group_name, description FROM pipeline_config WHERE key = %s",
                (key,),
            )
            row = cur.fetchone()
    return (row[0], row[1]) if row else None
