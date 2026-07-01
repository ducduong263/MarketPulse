"""
State persistence for alert-monitor.
Saves active_alerts and recovery cooldown timestamps to a JSON file
so the monitor survives container restarts without re-alerting known issues.
"""

import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_STATE_FILE = os.getenv("MONITOR_STATE_FILE", "/data/monitor_state.json")

_default_state = {
    "active_alerts": [],       # list of alert keys currently firing
    "last_recovery_ts": {},    # {alert_key: ISO timestamp of last recovery sent}
    "last_price_anomaly_ts": "1970-01-01T00:00:00+00:00",
    "last_spread_anomaly_ts": "1970-01-01T00:00:00+00:00",
}


def _load() -> dict:
    try:
        with open(_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Ensure keys exist for forward-compat
            for k, v in _default_state.items():
                data.setdefault(k, v)
            return data
    except FileNotFoundError:
        return dict(_default_state)
    except Exception as exc:
        logger.warning("Could not load state file, using fresh state: %s", exc)
        return dict(_default_state)


def _save(state: dict) -> None:
    try:
        os.makedirs(os.path.dirname(_STATE_FILE), exist_ok=True)
        with open(_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, default=str)
    except Exception as exc:
        logger.error("Could not save state file: %s", exc)


class AlertState:
    """
    Manages active alert tracking and recovery cooldowns.

    Usage:
        state = AlertState()
        if state.is_new(key):
            send_alert(...)
            state.mark_active(key)
        elif state.is_resolved(key) and state.can_recover(key, cooldown_minutes=5):
            send_recovery(...)
            state.mark_resolved(key)
    """

    def __init__(self):
        data = _load()
        self._active: set[str] = set(data["active_alerts"])
        self._last_recovery: dict[str, str] = data["last_recovery_ts"]
        self._last_price_anomaly_ts: str = data["last_price_anomaly_ts"]
        self._last_spread_anomaly_ts: str = data["last_spread_anomaly_ts"]

    @property
    def last_price_anomaly_ts(self) -> datetime:
        return datetime.fromisoformat(self._last_price_anomaly_ts)

    @last_price_anomaly_ts.setter
    def last_price_anomaly_ts(self, val: datetime) -> None:
        self._last_price_anomaly_ts = val.isoformat()
        self._flush()

    @property
    def last_spread_anomaly_ts(self) -> datetime:
        return datetime.fromisoformat(self._last_spread_anomaly_ts)

    @last_spread_anomaly_ts.setter
    def last_spread_anomaly_ts(self, val: datetime) -> None:
        self._last_spread_anomaly_ts = val.isoformat()
        self._flush()

    def is_active(self, key: str) -> bool:
        return key in self._active

    def is_new_alert(self, key: str) -> bool:
        """True if alert key is firing now but was not active before."""
        return key not in self._active

    def is_resolved(self, key: str) -> bool:
        """True if alert key was active but is no longer firing."""
        return key in self._active

    def can_send_recovery(self, key: str, cooldown_minutes: int = 5) -> bool:
        """
        True if enough time has passed since the last recovery message
        for this key (prevents flapping spam).
        """
        last_ts_str = self._last_recovery.get(key)
        if not last_ts_str:
            return True
        try:
            last_ts = datetime.fromisoformat(last_ts_str)
            elapsed = (datetime.now(timezone.utc) - last_ts).total_seconds()
            return elapsed >= cooldown_minutes * 60
        except Exception:
            return True

    def mark_active(self, key: str) -> None:
        self._active.add(key)
        self._flush()

    def mark_resolved(self, key: str) -> None:
        self._active.discard(key)
        self._last_recovery[key] = datetime.now(timezone.utc).isoformat()
        self._flush()

    def _flush(self) -> None:
        _save({
            "active_alerts": list(self._active),
            "last_recovery_ts": self._last_recovery,
            "last_price_anomaly_ts": self._last_price_anomaly_ts,
            "last_spread_anomaly_ts": self._last_spread_anomaly_ts,
        })
