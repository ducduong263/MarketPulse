"""
utils/delta_writer.py — Delta Lake write/optimize helpers for MarketPulse DAGs.

Provides:
- write_to_delta(df, table_uri, partition_col) : write pandas DataFrame → Delta Lake on MinIO
- optimize_delta_table(uri, date_str, z_order)  : COMPACT + Z-ORDER a Delta table partition
- snapshot_secdef_to_delta(today)               : snapshot security_definition → Delta Lake
"""

from __future__ import annotations

import os
import logging
import typing

if typing.TYPE_CHECKING:
    import datetime
    import pandas as pd
    import pyarrow as pa

logger = logging.getLogger(__name__)

# ── MinIO / Delta Lake config ─────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS   = os.getenv("minio_root_user",     "minioadmin")
MINIO_SECRET   = os.getenv("minio_root_password", "minioadmin")

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL":      f"http://{MINIO_ENDPOINT}",
    "AWS_ACCESS_KEY_ID":     MINIO_ACCESS,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET,
    "AWS_REGION":            "us-east-1",
    "AWS_ALLOW_HTTP":        "true",
}


def write_to_delta(
    df: pd.DataFrame,
    table_uri: str,
    partition_col: str = "date",
    mode: str = "append",
    schema: pa.Schema | None = None,
) -> int:
    """
    Write a pandas DataFrame to a Delta Lake table on MinIO.

    Args:
        df:            DataFrame to write.
        table_uri:     Delta table URI, e.g. "s3://market-data/bronze/market_trade"
        partition_col: Column name to partition by. Defaults to "date".
        mode:          "append" or "overwrite". Defaults to "append".
        schema:        Optional PyArrow schema. If None, inferred from DataFrame.

    Returns:
        Number of rows written.
    """
    import pandas as pd
    import pyarrow as pa
    from deltalake import write_deltalake

    if df.empty:
        logger.info(f"Empty DataFrame — skipping write to {table_uri}")
        return 0

    n = len(df)

    kwargs: typing.Dict[str, typing.Any] = {
        "mode":            mode,
        "storage_options": STORAGE_OPTIONS,
        "schema_mode":     "merge",
    }
    if partition_col and partition_col in df.columns:
        kwargs["partition_by"] = [partition_col]
    if schema is not None:
        table = pa.Table.from_pandas(df, schema=schema)
    else:
        table = pa.Table.from_pandas(df)

    write_deltalake(table_uri, table, **kwargs)
    logger.info(f"[DELTA] Wrote {n} rows -> {table_uri} (mode={mode})")
    return n


_SECDEF_DELTA_URI = "s3://market-data/bronze/security_definition"


def snapshot_secdef_to_delta(today: "datetime.date") -> int:
    """
    Read today's security_definition rows from TimescaleDB and
    write as a daily snapshot to Delta Lake Bronze.

    Encapsulates: DB connection → pandas query → Delta write.
    Callers (DAG tasks) do not need to import pandas or psycopg2.

    Args:
        today: The trading date to snapshot (ICT date).

    Returns:
        Number of rows written to Delta Lake (0 if no data found).
    """
    import pandas as pd
    from .db import get_db_conn

    logger.info("Snapshotting security_definition for %s → Delta Lake", today)

    with get_db_conn() as conn:
        df = pd.read_sql(
            "SELECT * FROM security_definition WHERE trading_date = %s",
            conn,
            params=(today,),
        )

    if df.empty:
        logger.warning("No security_definition rows for %s — skipping Delta snapshot", today)
        return 0

    df["date"] = today.isoformat()

    n = write_to_delta(df, table_uri=_SECDEF_DELTA_URI, partition_col="date", mode="append")
    logger.info("Snapshotted %d secdef rows for %s → %s", n, today, _SECDEF_DELTA_URI)
    return n


def optimize_delta_table(
    uri:       str,
    date_str:  str,
    z_order_cols: list[str] | None = None,
) -> dict:
    """
    Run COMPACT (and optionally Z-ORDER) on a Delta table partition.

    Args:
        uri:          Delta table URI, e.g. "s3://market-data/bronze/market_trade".
        date_str:     ISO date string for the partition filter, e.g. "2026-05-26".
        z_order_cols: Column(s) to Z-ORDER by. If None, only COMPACT is run.

    Returns:
        Dict with keys 'compact' and (if applicable) 'z_order' containing metric strings.
    """
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError

    logger.info("Optimizing %s (date=%s) ...", uri, date_str)

    try:
        dt = DeltaTable(uri, storage_options=STORAGE_OPTIONS)
    except TableNotFoundError:
        logger.warning("[SKIP] Table does not exist yet: %s", uri)
        return {"status": "skipped", "reason": "TableNotFoundError"}

    try:
        compact_metrics = dt.optimize.compact(partition_filters=[("date", "=", date_str)])
        logger.info("[COMPACT] %s: %s", uri, compact_metrics)
        result = {"compact": str(compact_metrics)}

        if z_order_cols:
            z_metrics = dt.optimize.z_order(z_order_cols, partition_filters=[("date", "=", date_str)])
            logger.info("[Z-ORDER] %s by %s: %s", uri, z_order_cols, z_metrics)
            result["z_order"] = str(z_metrics)

        return result
    except Exception as e:
        logger.error("[ERROR] Optimization failed for %s: %s", uri, e)
        return {"status": "error", "reason": str(e)}


def vacuum_delta_table(
    uri: str,
    retention_hours: int = 48,
) -> dict:
    """
    Remove tombstoned (logically deleted) physical files from a Delta table.

    Delta Lake never deletes files immediately after COMPACT/DELETE — it marks
    them as removed in the transaction log and keeps them on disk for time travel.
    VACUUM physically deletes files older than `retention_hours`.

    Args:
        uri:               Delta table URI.
        retention_hours:   Minimum age (in hours) of files to delete. Defaults to 48h.
                           Must be >= 1 to avoid deleting files mid-transaction.

    Returns:
        Dict with 'deleted_files' count and 'status'.
    """
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError

    logger.info("VACUUM %s (retention=%dh) ...", uri, retention_hours)

    try:
        dt = DeltaTable(uri, storage_options=STORAGE_OPTIONS)
    except TableNotFoundError:
        logger.warning("[SKIP] Table does not exist yet: %s", uri)
        return {"status": "skipped", "reason": "TableNotFoundError"}

    try:
        deleted = dt.vacuum(
            retention_hours=retention_hours,
            dry_run=False,
            enforce_retention_duration=True,
        )
        count = len(deleted)
        logger.info("[VACUUM] %s: deleted %d files", uri, count)
        return {"status": "ok", "deleted_files": count}
    except Exception as e:
        logger.error("[ERROR] VACUUM failed for %s: %s", uri, e)
        return {"status": "error", "reason": str(e)}

