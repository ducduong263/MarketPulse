"""
ingestion/common/stats_reporter.py

Background thread that periodically flushes service health metrics
into the `pipeline_stats` TimescaleDB table.

Design decisions:
- Runs in a daemon thread so it doesn't block the main producer/consumer loop.
- Uses a simple in-memory counter dict; callers update counters via thread-safe methods.
- Flush interval: 30 seconds (configurable via STATS_FLUSH_INTERVAL env var).
- DB connection is separate from the consumer's main connection to avoid
  transaction interference.
- Silently swallows DB errors so a monitoring failure never kills the pipeline.
- Connection tracking: mark_online() / mark_offline() write connection_status=1/0
  immediately (not buffered). Grafana State Timeline panel reads these state
  transitions and automatically calculates how long each outage lasted.
"""
from __future__ import annotations

import os
import threading
import time
from typing import TYPE_CHECKING

import psycopg2

if TYPE_CHECKING:
    pass

_INSERT_SQL = """
    INSERT INTO pipeline_stats (ts, service_name, metric_name, metric_value, label)
    VALUES (NOW(), %s, %s, %s, %s)
"""

_DEFAULT_FLUSH_INTERVAL = 30  # seconds — used if both ConfigStore and env are unavailable


def _get_flush_interval() -> int:
    """Read flush interval from ConfigStore (dynamic) or env fallback."""
    try:
        from ingestion.common.config_store import get_config_store
        store = get_config_store()
        v = store.get_int("stats_flush_interval", default=0)
        if v > 0:
            return v
    except Exception:
        pass
    return int(os.getenv("STATS_FLUSH_INTERVAL", str(_DEFAULT_FLUSH_INTERVAL)))


class StatsReporter:
    """
    Thread-safe metrics accumulator + periodic DB flusher.

    Usage:
        reporter = StatsReporter(service_name="p-trade")
        reporter.start()

        # In producer/consumer code:
        reporter.inc_msg()               # count a message produced/consumed
        reporter.inc_avro_error()        # count a serialization error
        reporter.mark_online()           # write connection_status=1 immediately
        reporter.mark_offline()          # write connection_status=0 immediately
        reporter.set_consumer_lag(lag, topic)  # set current lag (consumers only)

        reporter.stop()  # on shutdown
    """

    def __init__(self, service_name: str) -> None:
        self.service_name = service_name
        self._lock = threading.Lock()

        # Counters (reset every flush interval for rate calculation)
        self._msg_count_window: int = 0    # messages in current window
        self._avro_errors: int = 0

        # Gauges (current value, not reset)
        self._ws_connected: int = 0        # 1 = connected, 0 = disconnected
        self._reconnect_total: int = 0     # monotonically increasing
        self._consumer_lag: dict[str, float] = {}  # topic -> lag

        self._window_start = time.monotonic()
        self._running = False
        self._thread: threading.Thread | None = None

    # ── Public API (called from producer/consumer thread) ──────────

    def inc_msg(self, n: int = 1) -> None:
        with self._lock:
            self._msg_count_window += n

    def inc_avro_error(self, n: int = 1) -> None:
        with self._lock:
            self._avro_errors += n

    def set_consumer_lag(self, lag: float, topic: str = "") -> None:
        with self._lock:
            self._consumer_lag[topic] = lag

    def mark_online(self) -> None:
        """Call when WS connects or reconnects. Writes connection_status=1 immediately."""
        with self._lock:
            self._ws_connected = 1
            self._reconnect_total += 1
        self._write_to_db([
            (self.service_name, "connection_status", 1.0, None),
        ])
        print(f"[ONLINE] {self.service_name} WS connected (reconnect #{self._reconnect_total})")

    def mark_offline(self) -> None:
        """Call when WS drops or service shuts down. Writes connection_status=0 immediately."""
        with self._lock:
            self._ws_connected = 0
        self._write_to_db([
            (self.service_name, "connection_status", 0.0, None),
        ])
        print(f"[OFFLINE] {self.service_name} WS disconnected")

    # ── Lifecycle ─────────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(
            target=self._flush_loop,
            name=f"stats-{self.service_name}",
            daemon=True,
        )
        self._thread.start()
        print(f"[STATS] Reporter started for {self.service_name} (interval=dynamic, default={_DEFAULT_FLUSH_INTERVAL}s)")

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        # Final flush
        self._do_flush()

    # ── Internal ──────────────────────────────────────────────────

    def _flush_loop(self) -> None:
        while self._running:
            interval = _get_flush_interval()
            time.sleep(interval)
            self._do_flush()

    def _do_flush(self) -> None:
        now = time.monotonic()
        elapsed = now - self._window_start

        with self._lock:
            msg_count = self._msg_count_window
            avro_errors = self._avro_errors
            ws_connected = self._ws_connected
            reconnect_total = self._reconnect_total
            consumer_lag = dict(self._consumer_lag)

            # Reset window counters
            self._msg_count_window = 0
            self._avro_errors = 0

        self._window_start = now

        # Calculate rate (messages per second)
        msg_per_sec = msg_count / max(elapsed, 1)

        rows: list[tuple[str, str, float, str | None]] = [
            (self.service_name, "reconnect_count",  float(reconnect_total), None),
            (self.service_name, "msg_per_sec",      round(msg_per_sec, 4),  None),
            (self.service_name, "avro_error_count", float(avro_errors),     None),
            (self.service_name, "ws_connected",     float(ws_connected),    None),
        ]

        # Add per-topic consumer lag rows
        for topic, lag in consumer_lag.items():
            rows.append((self.service_name, "consumer_lag", lag, topic))

        self._write_to_db(rows)

    def _write_to_db(self, rows: list[tuple]) -> None:
        conn = None
        try:
            conn = psycopg2.connect(
                host=os.getenv("postgres_host", "localhost"),
                port=os.getenv("postgres_port", "5432"),
                dbname=os.getenv("postgres_db", "market_data"),
                user=os.getenv("postgres_user", "marketpulse"),
                password=os.getenv("postgres_password", "mp_secret_2026"),
                connect_timeout=5,
            )
            with conn.cursor() as cur:
                cur.executemany(_INSERT_SQL, rows)
            conn.commit()
        except Exception as e:
            # Monitoring failure must never kill the pipeline
            print(f"[STATS][WARN] Failed to write metrics to DB: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
