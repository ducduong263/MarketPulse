"""
transform/silver/transform_ohlc.py

Silver Layer OHLC Transform.

Reads Bronze OHLC from Delta Lake on MinIO, fills missing candles using forward-fill,
enriches with metadata (is_filled, trading_date, bar_type), and writes to Silver Delta Lake.

The fill logic uses asset-specific time spines:
  - DERIVATIVE (VN30F1M, VN30F2M, etc.) : 09:00 - 11:29 + 13:00 - 14:29 + 14:45 (ATC)
  - STOCK / INDEX                         : 09:15 - 11:29 + 13:00 - 14:29 + 14:45 (ATC)

Resolution 1D does NOT need fill (only 1 candle per day) -- metadata-only enrichment.

CLI Usage:
  python transform/silver/transform_ohlc.py --date 2026-06-30
  python transform/silver/transform_ohlc.py --date 2026-06-30 --symbols VCB VN30F1M --overwrite
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from dotenv import load_dotenv

# ── Path setup (works both from CLI and when imported by Airflow DAG) ─────────
# transform/ is at the same level as ingestion/, both are children of project root.
_ROOT_CANDIDATES = [
    Path(__file__).resolve().parents[2],   # transform/../  => project root
    Path("/opt/airflow"),
]
for _root in _ROOT_CANDIDATES:
    if _root.exists() and str(_root) not in sys.path:
        sys.path.insert(0, str(_root))
        break

load_dotenv()

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT") or os.getenv("minio_endpoint") or "localhost:9005"
MINIO_USER     = os.getenv("minio_root_user") or os.getenv("MINIO_ROOT_USER") or "minioadmin"
MINIO_PASSWORD = os.getenv("minio_root_password") or os.getenv("MINIO_ROOT_PASSWORD") or "minioadmin"

BRONZE_URI = "s3://market-data/bronze/ohlc"
SILVER_URI = "s3://market-data/silver/ohlc"

ICT_TZ = timezone(timedelta(hours=7))


def _build_storage_options() -> dict:
    return {
        "AWS_ENDPOINT_URL":           f"http://{MINIO_ENDPOINT}",
        "AWS_ACCESS_KEY_ID":          MINIO_USER,
        "AWS_SECRET_ACCESS_KEY":      MINIO_PASSWORD,
        "AWS_REGION":                 "us-east-1",
        "AWS_ALLOW_HTTP":             "true",
        "AWS_FORCE_PATH_STYLE":       "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def _get_bar_type(symbol: str) -> str:
    """Classify a symbol into STOCK, DERIVATIVE, or INDEX."""
    s = symbol.upper().strip()
    if s in {"VN30", "VNINDEX", "HNX", "HNX30", "VN100", "UPCOM"}:
        return "INDEX"
    if (s.startswith("VN30F") or s.startswith("V100F") or
            s.startswith("41I1") or s.startswith("41I2")):
        return "DERIVATIVE"
    return "STOCK"


# ── Silver Arrow Schema ───────────────────────────────────────────────────────
SILVER_SCHEMA = pa.schema([
    ("time",         pa.timestamp("us", tz="UTC")),
    ("symbol",       pa.string()),
    ("resolution",   pa.string()),
    ("open",         pa.float64()),
    ("high",         pa.float64()),
    ("low",          pa.float64()),
    ("close",        pa.float64()),
    ("volume",       pa.int64()),
    ("bar_type",     pa.string()),
    ("is_filled",    pa.bool_()),
    ("trading_date", pa.string()),    # ICT date string, e.g. "2026-06-30"
    ("date",         pa.string()),    # partition column (same as trading_date)
])


# ── 1-minute trading session time spines ─────────────────────────────────────

def _generate_trading_minutes_derivative(trading_date: str) -> list[datetime]:
    """
    Generate all valid 1-minute bar timestamps (UTC) for DERIVATIVE on a trading day.
    DVX opens at 09:00 ICT:
      Morning  : 09:00 - 11:29 (90 bars)
      Afternoon: 13:00 - 14:29 (90 bars)
      ATC bar  : 14:45
    Total: 181 bars per day.
    """
    d = date.fromisoformat(trading_date)
    bars = []
    start_am = datetime(d.year, d.month, d.day, 9, 0, tzinfo=ICT_TZ)
    for m in range(90):
        bars.append((start_am + timedelta(minutes=m)).astimezone(timezone.utc))
    start_pm = datetime(d.year, d.month, d.day, 13, 0, tzinfo=ICT_TZ)
    for m in range(90):
        bars.append((start_pm + timedelta(minutes=m)).astimezone(timezone.utc))
    atc = datetime(d.year, d.month, d.day, 14, 45, tzinfo=ICT_TZ)
    bars.append(atc.astimezone(timezone.utc))
    return bars  # 181 bars


def _generate_trading_minutes_stock(trading_date: str) -> list[datetime]:
    """
    Generate all valid 1-minute bar timestamps (UTC) for STOCK / INDEX on a trading day.
    HOSE opens at 09:15 ICT:
      Morning  : 09:15 - 11:29 (75 bars)
      Afternoon: 13:00 - 14:29 (90 bars)
      ATC bar  : 14:45
    Total: 166 bars per day.
    """
    d = date.fromisoformat(trading_date)
    bars = []
    start_am = datetime(d.year, d.month, d.day, 9, 15, tzinfo=ICT_TZ)
    for m in range(75):
        bars.append((start_am + timedelta(minutes=m)).astimezone(timezone.utc))
    start_pm = datetime(d.year, d.month, d.day, 13, 0, tzinfo=ICT_TZ)
    for m in range(90):
        bars.append((start_pm + timedelta(minutes=m)).astimezone(timezone.utc))
    atc = datetime(d.year, d.month, d.day, 14, 45, tzinfo=ICT_TZ)
    bars.append(atc.astimezone(timezone.utc))
    return bars  # 166 bars


# ── Core transform per symbol ─────────────────────────────────────────────────

def _transform_1min_symbol(
    symbol: str,
    trading_date: str,
    bronze_df: "pd.DataFrame",
) -> pa.Table:
    """
    Fill missing 1-minute candles for a single symbol on a single trading day.
    Forward-fill close price into missing open/high/low/close. Volume = 0 for fills.
    If a symbol has NO candles at all, all bars will have NULL OHLC + is_filled=True.
    """
    import pandas as pd

    bar_type = _get_bar_type(symbol)
    expected_times = (
        _generate_trading_minutes_derivative(trading_date)
        if bar_type == "DERIVATIVE"
        else _generate_trading_minutes_stock(trading_date)
    )

    sym_df = bronze_df[bronze_df["symbol"] == symbol].copy()
    sym_df = sym_df.set_index("time").sort_index()

    rows = []
    last_close = None

    for ts_utc in expected_times:
        ts_key = pd.Timestamp(ts_utc).tz_convert("UTC")

        if ts_key in sym_df.index:
            row = sym_df.loc[ts_key]
            last_close = float(row["close"])
            rows.append({
                "time":         ts_utc,
                "symbol":       symbol,
                "resolution":   "1min",
                "open":         float(row["open"]),
                "high":         float(row["high"]),
                "low":          float(row["low"]),
                "close":        last_close,
                "volume":       int(row["volume"]),
                "bar_type":     bar_type,
                "is_filled":    False,
                "trading_date": trading_date,
                "date":         trading_date,
            })
        else:
            rows.append({
                "time":         ts_utc,
                "symbol":       symbol,
                "resolution":   "1min",
                "open":         last_close,
                "high":         last_close,
                "low":          last_close,
                "close":        last_close,
                "volume":       0,
                "bar_type":     bar_type,
                "is_filled":    True,
                "trading_date": trading_date,
                "date":         trading_date,
            })

    result_df = pd.DataFrame(rows)
    result_df["time"] = pd.to_datetime(result_df["time"], utc=True)
    return pa.Table.from_pandas(result_df, schema=SILVER_SCHEMA, preserve_index=False)


def _transform_1D_symbol(
    symbol: str,
    trading_date: str,
    bronze_df: "pd.DataFrame",
) -> "pa.Table | None":
    """
    Enrich 1D (daily) candles with Silver metadata.
    Resolution 1D does NOT need fill -- 0 or 1 bar per day.
    Returns None if no data for this symbol on this date.
    """
    import pandas as pd

    bar_type = _get_bar_type(symbol)
    sym_df = bronze_df[bronze_df["symbol"] == symbol]

    if sym_df.empty:
        return None

    rows = []
    for _, row in sym_df.iterrows():
        rows.append({
            "time":         row["time"],
            "symbol":       symbol,
            "resolution":   "1D",
            "open":         float(row["open"]),
            "high":         float(row["high"]),
            "low":          float(row["low"]),
            "close":        float(row["close"]),
            "volume":       int(row["volume"]),
            "bar_type":     bar_type,
            "is_filled":    False,
            "trading_date": trading_date,
            "date":         trading_date,
        })

    result_df = pd.DataFrame(rows)
    result_df["time"] = pd.to_datetime(result_df["time"], utc=True)
    return pa.Table.from_pandas(result_df, schema=SILVER_SCHEMA, preserve_index=False)


# ── I/O helpers ───────────────────────────────────────────────────────────────

def _read_bronze(trading_date: str, resolution: str, symbols: list[str] | None) -> "pd.DataFrame":
    """Read Bronze OHLC for a specific date + resolution. Returns UTC-aware pandas DataFrame."""
    import pandas as pd

    storage_options = _build_storage_options()
    try:
        dt = DeltaTable(BRONZE_URI, storage_options=storage_options)
    except TableNotFoundError:
        logger.warning("[SILVER] Bronze table not found: %s", BRONZE_URI)
        return pd.DataFrame(columns=["time", "symbol", "resolution", "open", "high", "low", "close", "volume", "date"])

    filters = [("date", "=", trading_date), ("resolution", "=", resolution)]
    table = dt.to_pyarrow_table(filters=filters)
    df = table.to_pandas()

    if symbols:
        df = df[df["symbol"].isin(symbols)]

    if df.empty:
        return df

    if df["time"].dt.tz is None:
        df["time"] = df["time"].dt.tz_localize("UTC")
    else:
        df["time"] = df["time"].dt.tz_convert("UTC")

    return df


def _silver_already_has_data(trading_date: str, resolution: str, symbol: str) -> bool:
    """Return True if Silver already has data for this symbol/date/resolution."""
    storage_options = _build_storage_options()
    try:
        dt = DeltaTable(SILVER_URI, storage_options=storage_options)
        tbl = dt.to_pyarrow_table(
            columns=["time"],
            filters=[
                ("date",       "=", trading_date),
                ("resolution", "=", resolution),
                ("symbol",     "=", symbol),
            ],
        )
        return len(tbl) > 0
    except Exception:
        return False


def _write_silver(table: pa.Table, overwrite_predicate: str | None = None) -> None:
    """Append a PyArrow Table to Silver Delta Lake, optionally deleting stale rows first."""
    storage_options = _build_storage_options()

    if overwrite_predicate:
        try:
            dt = DeltaTable(SILVER_URI, storage_options=storage_options)
            dt.delete(overwrite_predicate)
            logger.info("[SILVER] Deleted existing rows: %s", overwrite_predicate)
        except TableNotFoundError:
            pass
        except Exception as e:
            logger.warning("[SILVER] Could not delete old silver rows: %s", e)

    write_deltalake(
        SILVER_URI,
        table,
        mode="append",
        partition_by=["symbol", "resolution"],
        storage_options=storage_options,
        schema_mode="merge",
    )


# ── Public API ────────────────────────────────────────────────────────────────

def run_silver_transform(
    date_str: str,
    symbols: list[str] | None = None,
    resolutions: list[str] | None = None,
    overwrite: bool = False,
) -> dict:
    """
    Main entry point for Silver OHLC transform.

    Args:
        date_str    : Trading date in "YYYY-MM-DD" (ICT).
        symbols     : Symbols to process. None = all symbols present in Bronze for that date.
        resolutions : ["1min", "1D"] or a subset. None = both.
        overwrite   : Delete existing Silver rows for these symbols/date before writing.

    Returns:
        Stats dict: {resolution: {"symbols": int, "rows_written": int, "skipped": int}}
    """
    if resolutions is None:
        resolutions = ["1min", "1D"]

    logger.info(
        "[SILVER] date=%s symbols=%s resolutions=%s overwrite=%s",
        date_str, symbols, resolutions, overwrite,
    )

    stats: dict = {}

    for resolution in resolutions:
        logger.info("[SILVER] Processing resolution: %s", resolution)
        stats[resolution] = {"symbols": 0, "rows_written": 0, "skipped": 0}

        bronze_df = _read_bronze(date_str, resolution, symbols)

        if bronze_df.empty:
            logger.warning("[SILVER] No Bronze data: date=%s resolution=%s", date_str, resolution)
            continue

        all_symbols = sorted(bronze_df["symbol"].unique()) if symbols is None else symbols
        logger.info("[SILVER] %d symbols to process for %s", len(all_symbols), resolution)

        tables_to_write: list[pa.Table] = []

        for sym in all_symbols:
            if not overwrite and _silver_already_has_data(date_str, resolution, sym):
                stats[resolution]["skipped"] += 1
                continue

            if resolution == "1min":
                tbl = _transform_1min_symbol(sym, date_str, bronze_df)
            elif resolution == "1D":
                tbl = _transform_1D_symbol(sym, date_str, bronze_df)
                if tbl is None:
                    stats[resolution]["skipped"] += 1
                    continue
            else:
                logger.warning("[SILVER] Unsupported resolution: %s", resolution)
                continue

            tables_to_write.append(tbl)
            stats[resolution]["symbols"] += 1
            stats[resolution]["rows_written"] += len(tbl)

        if not tables_to_write:
            logger.info("[SILVER] Nothing to write for resolution=%s", resolution)
            continue

        combined = pa.concat_tables(tables_to_write)

        overwrite_pred = None
        if overwrite:
            sym_list = ", ".join(f"'{s}'" for s in all_symbols)
            overwrite_pred = (
                f"date = '{date_str}' AND resolution = '{resolution}' "
                f"AND symbol IN ({sym_list})"
            )

        _write_silver(combined, overwrite_predicate=overwrite_pred)
        logger.info(
            "[SILVER] Wrote %d rows for resolution=%s (%d symbols)",
            len(combined), resolution, stats[resolution]["symbols"],
        )

    logger.info("[SILVER] Done. Stats: %s", stats)
    return stats


# ── CLI Entry Point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Silver Layer OHLC transform.")
    parser.add_argument("--date",        type=str, required=True, help="Trading date YYYY-MM-DD (ICT)")
    parser.add_argument("--symbols",     type=str, nargs="*",     help="Symbols to process (default: all)")
    parser.add_argument("--resolutions", type=str, nargs="*",     help="Resolutions: 1min 1D (default: both)")
    parser.add_argument("--overwrite",   action="store_true",     help="Overwrite existing Silver data")
    args = parser.parse_args()

    result = run_silver_transform(
        date_str=args.date,
        symbols=args.symbols or None,
        resolutions=args.resolutions or None,
        overwrite=args.overwrite,
    )
    print("Transform result:", result)
