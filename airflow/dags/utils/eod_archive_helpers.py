"""
utils/eod_archive_helpers.py — End-of-day archive helpers for MarketPulse DAGs.

Provides:
- archive_topic_to_delta(topic, schema_file, delta_uri, ts_cols, group_id)
  : Read a Kafka topic from 09:00 ICT today → write to Delta Lake Bronze.
- today_ict_start()
  : Return today's 09:00 ICT as a UTC datetime (used for Kafka seek).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

SCHEMAS_DIR = Path("/opt/airflow/schemas")


def today_ict_start() -> datetime:
    """
    Return the start of today's trading session as a UTC datetime.
    09:00 ICT = 02:00 UTC.
    """
    now_utc     = datetime.now(timezone.utc)
    today_ict   = (now_utc + timedelta(hours=7)).date()
    return datetime(today_ict.year, today_ict.month, today_ict.day, 2, 0, 0, tzinfo=timezone.utc)


def archive_topic_to_delta(
    topic:       str,
    schema_file: str,
    delta_uri:   str,
    ts_cols:     list[str],
    group_id:    str,
) -> int:
    """
    Read a Kafka topic from 09:00 ICT today and write records to Delta Lake.

    Args:
        topic:       Kafka topic name.
        schema_file: Avro schema filename (relative to SCHEMAS_DIR).
        delta_uri:   Delta Lake target URI, e.g. "s3://market-data/bronze/...".
        ts_cols:     Timestamp column names to convert from epoch-ms → UTC datetime.
        group_id:    Kafka consumer group ID (unique per topic/DAG).

    Returns:
        Number of rows written to Delta Lake (0 if no records found).
    """
    from .kafka_reader import read_kafka_from_timestamp
    from .delta_writer import write_to_delta

    schema_str = (SCHEMAS_DIR / schema_file).read_text()
    start_ts   = today_ict_start()
    today_str  = (datetime.now(timezone.utc) + timedelta(hours=7)).date().isoformat()

    logger.info("Reading %s from %s (group=%s)...", topic, start_ts.isoformat(), group_id)
    records = read_kafka_from_timestamp(
        topic=topic,
        start_ts=start_ts,
        schema_str=schema_str,
        group_id=group_id,
    )

    if not records:
        logger.warning("No records found in %s for %s", topic, today_str)
        return 0

    import pandas as pd
    df = pd.DataFrame(records)
    df["date"] = today_str

    # Normalize epoch-ms timestamp columns → UTC datetime
    for col in ts_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="ms", utc=True, errors="coerce")

    n = write_to_delta(df, delta_uri, partition_col="date")
    logger.info("[%s] Archived %d rows for %s → %s", topic, n, today_str, delta_uri)
    return n
