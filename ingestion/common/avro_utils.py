"""
ingestion/common/avro_utils.py

Shared Avro deserialization helpers used by consumers and archivers.
"""
from __future__ import annotations

from datetime import datetime, timezone


def unwrap_union(value):
    """
    Avro union ["null", "type"] is deserialized as {"type": value} or None.
    Unwrap to the actual value.

    Examples:
        {"long": 29}     -> 29
        {"double": 1.5}  -> 1.5
        None             -> None
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return next(iter(value.values()))
    return value


def ms_to_ts(value) -> datetime | None:
    """
    Avro timestamp-millis (int | datetime) -> timezone-aware datetime UTC.

    Used by consumers writing to TimescaleDB.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def to_ts(value) -> datetime | None:
    """
    Like ms_to_ts but also handles naive datetime (adds UTC tzinfo).

    Used by archivers writing to Delta Lake (PyArrow requires tz-aware).
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
