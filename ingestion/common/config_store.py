"""
ingestion/common/config_store.py

Thread-safe, DB-backed dynamic configuration store.

Design:
  - Singleton: toàn bộ process dùng chung 1 instance (gọi get_config_store()).
  - Polling: background thread poll DB theo từng group với interval riêng.
      * symbol_filter group: 60s  — nhay cam gio giao dich
      * flush / connection groups: 300s
  - Thread safety: threading.Lock() + copy() dict trước khi swap.
  - Fallback: nếu DB down, giữ nguyên config cũ, không raise exception.
  - Bootstrap: lần đầu khởi tạo đọc ngay từ DB; nếu DB down thì fallback về os.getenv().

Usage:
    from ingestion.common.config_store import get_config_store

    store = get_config_store()  # singleton
    store.get('symbol_filter_indexes', default='VN30')
    store.get_list('symbol_filter_indexes', default=['VN30'])
    store.get_float('flush_timeout_seconds', default=2.0)
    store.get_int('flush_batch_size', default=100)

Environment variable fallback order:
    1. pipeline_config table in DB  (primary)
    2. os.getenv(key.upper(), default)  (fallback when DB unavailable)
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

# ── Poll intervals by group_name ──────────────────────────────────────────────
_POLL_INTERVALS: dict[str, int] = {
    "symbol_filter": 60,    # 1 min — sensitive during trading hours
    "flush":         300,   # 5 min — less urgent
    "connection":    300,   # 5 min
}
_DEFAULT_POLL_INTERVAL = 300  # for any unknown group


# ── DB connection helper ──────────────────────────────────────────────────────
def _make_conn():
    import psycopg2
    return psycopg2.connect(
        host=os.getenv("postgres_host",     "localhost"),
        port=int(os.getenv("postgres_port", "5432")),
        dbname=os.getenv("postgres_db",     "market_data"),
        user=os.getenv("postgres_user",     "marketpulse"),
        password=os.getenv("postgres_password", "mp_secret_2026"),
        connect_timeout=5,
    )


# ── ConfigStore ───────────────────────────────────────────────────────────────
class ConfigStore:
    """
    Thread-safe config cache backed by pipeline_config table.

    Internally stores config as a flat dict[str, str] with string values.
    Caller uses typed accessors (get, get_int, get_float, get_list).
    """

    def __init__(self) -> None:
        self._config: dict[str, str] = {}
        self._lock = threading.Lock()
        self._running = False
        self._threads: list[threading.Thread] = []
        self._last_poll: dict[str, float] = {}   # group_name -> monotonic timestamp

        # Load config from env as initial fallback (before first DB poll)
        self._load_env_fallback()

        # Attempt immediate DB load on startup
        try:
            self._poll_all()
            logger.info("[ConfigStore] Initial load from DB successful (%d keys)", len(self._config))
        except Exception as e:
            logger.warning("[ConfigStore] Initial DB load failed, using env fallback: %s", e)

    # ── Typed accessors ───────────────────────────────────────────────────────

    def get(self, key: str, default: str = "") -> str:
        """Return config value as string. Falls back to default if key not found."""
        with self._lock:
            return self._config.get(key, default)

    def get_int(self, key: str, default: int = 0) -> int:
        """Return config value as int."""
        raw = self.get(key, "")
        try:
            return int(raw) if raw else default
        except (ValueError, TypeError):
            logger.warning("[ConfigStore] Cannot parse '%s'='%s' as int, using default=%s", key, raw, default)
            return default

    def get_float(self, key: str, default: float = 0.0) -> float:
        """Return config value as float."""
        raw = self.get(key, "")
        try:
            return float(raw) if raw else default
        except (ValueError, TypeError):
            logger.warning("[ConfigStore] Cannot parse '%s'='%s' as float, using default=%s", key, raw, default)
            return default

    def get_list(self, key: str, default: list[str] | None = None) -> list[str]:
        """Return comma-sep config value as list of stripped strings."""
        if default is None:
            default = []
        raw = self.get(key, "").strip()
        if not raw:
            return default
        return [s.strip() for s in raw.split(",") if s.strip()]

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """
        Start background polling threads, one per group.
        Safe to call multiple times (idempotent).
        """
        if self._running:
            return
        self._running = True

        groups = list(_POLL_INTERVALS.keys()) + ["default"]
        started: set[str] = set()

        for group, interval in _POLL_INTERVALS.items():
            if group in started:
                continue
            started.add(group)
            t = threading.Thread(
                target=self._poll_loop,
                args=(group, interval),
                name=f"config-poll-{group}",
                daemon=True,
            )
            t.start()
            self._threads.append(t)
            logger.info("[ConfigStore] Poll thread started: group=%s interval=%ds", group, interval)

    def stop(self) -> None:
        """Signal all poll threads to stop."""
        self._running = False

    # ── Internal ──────────────────────────────────────────────────────────────

    def _load_env_fallback(self) -> None:
        """Bootstrap config from os.getenv — used when DB is unavailable at startup."""
        env_defaults = {
            "symbol_filter_mode":     os.getenv("SYMBOL_FILTER_MODE",    "static"),
            "symbol_filter_indexes":  os.getenv("SYMBOL_FILTER_INDEXES", ""),
            "symbol_filter_groups":   os.getenv("SYMBOL_FILTER_GROUPS",  ""),
            "symbol_filter_status":   os.getenv("SYMBOL_FILTER_STATUS",  "NO_HALT"),
            "symbol_filter_admin":    os.getenv("SYMBOL_FILTER_ADMIN",   ""),
            "symbol_filter_sanction": os.getenv("SYMBOL_FILTER_SANCTION","NRM"),
            "symbol_filter_board_id": os.getenv("SYMBOL_FILTER_BOARD_ID","G1"),
            "symbol_filter_market":   os.getenv("SYMBOL_FILTER_MARKET",  ""),
            "flush_batch_size":       os.getenv("FLUSH_BATCH_SIZE",      "100"),
            "flush_timeout_seconds":  os.getenv("FLUSH_TIMEOUT_SECONDS", "2.0"),
            "stats_flush_interval":   os.getenv("STATS_FLUSH_INTERVAL",  "30"),
            "connection_timeout":     os.getenv("CONNECTION_TIMEOUT",    "5"),
        }
        with self._lock:
            self._config = {k: v for k, v in env_defaults.items() if v is not None}

    def _fetch_group_from_db(self, group: str) -> dict[str, str]:
        """Query pipeline_config for a specific group_name. Returns key->value dict."""
        conn = _make_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT key, value FROM pipeline_config WHERE group_name = %s",
                    (group,),
                )
                rows = cur.fetchall()
            return {row[0]: row[1] for row in rows}
        finally:
            conn.close()

    def _fetch_all_from_db(self) -> dict[str, str]:
        """Query all pipeline_config rows. Returns key->value dict."""
        conn = _make_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT key, value FROM pipeline_config")
                rows = cur.fetchall()
            return {row[0]: row[1] for row in rows}
        finally:
            conn.close()

    def _poll_all(self) -> None:
        """Fetch all config from DB and swap into memory. Used for initial load."""
        new_data = self._fetch_all_from_db()
        with self._lock:
            self._config = new_data.copy()

    def _poll_group(self, group: str) -> bool:
        """
        Fetch config for a specific group from DB and merge into memory.
        Returns True if any value changed, False otherwise.
        """
        new_group = self._fetch_group_from_db(group)
        if not new_group:
            return False

        changed = False
        with self._lock:
            for k, v in new_group.items():
                if self._config.get(k) != v:
                    logger.info(
                        "[ConfigStore] Config changed: %s = '%s' -> '%s'",
                        k, self._config.get(k, "<unset>"), v,
                    )
                    changed = True
                self._config[k] = v

        return changed

    def _poll_loop(self, group: str, interval: int) -> None:
        """Background thread: poll DB every `interval` seconds for a config group."""
        while self._running:
            time.sleep(interval)
            try:
                changed = self._poll_group(group)
                if changed:
                    logger.info("[ConfigStore] Reloaded group='%s' config from DB", group)
                else:
                    logger.debug("[ConfigStore] Poll group='%s': no changes", group)
            except Exception as e:
                # DB down — keep current config, never crash the producer
                logger.warning(
                    "[ConfigStore] Poll failed for group='%s', keeping old config: %s",
                    group, e,
                )


# ── Singleton accessor ────────────────────────────────────────────────────────
_instance: ConfigStore | None = None
_instance_lock = threading.Lock()


def get_config_store() -> ConfigStore:
    """
    Return the process-wide singleton ConfigStore.
    Thread-safe. Starts background polling on first call.
    """
    global _instance
    if _instance is None:
        with _instance_lock:
            if _instance is None:
                _instance = ConfigStore()
                _instance.start()
    return _instance
