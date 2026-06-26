"""
ingestion/handlers/backfill_ohlc.py

OHLC REST API Ingestion Handler for DNSE REST API to Delta Lake on MinIO.
Supports manual backfill CLI and end-of-day archiving DAG calls.

CLI Usage:
  python ingestion/handlers/backfill_ohlc.py \\
    --symbol VCB --resolution 1 \\
    --from "2026-06-23 09:00:00" --to "2026-06-23 15:00:00" \\
    --overwrite

EOD Usage:
  python -c "from ingestion.handlers.backfill_ohlc import run_eod_ohlc; run_eod_ohlc()"
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import psycopg2
from botocore.exceptions import ClientError
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
from dotenv import load_dotenv

# ── SDK path setup ────────────────────────────────────────────────
_SDK_CANDIDATES = [
    Path(__file__).resolve().parents[3] / "sdk" / "openapi-sdk" / "python",
    Path("/opt/airflow/sdk/openapi-sdk/python"),
]
for _sdk_path in _SDK_CANDIDATES:
    if _sdk_path.exists() and str(_sdk_path) not in sys.path:
        sys.path.insert(0, str(_sdk_path))
        break

_ROOT_CANDIDATES = [
    Path(__file__).resolve().parents[2],
    Path("/opt/airflow"),
]
for _root in _ROOT_CANDIDATES:
    if _root.exists() and str(_root) not in sys.path:
        sys.path.insert(0, str(_root))
        break

from dnse import DNSEClient  # noqa: E402
from ingestion import CheckpointManager  # noqa: E402

load_dotenv()

# ── Configs ───────────────────────────────────────────────────────
DNSE_API_KEY    = os.environ.get("DNSE_API_KEY", "")
DNSE_API_SECRET = os.environ.get("DNSE_API_SECRET", "")
DNSE_BASE_URL   = "https://openapi.dnse.com.vn"

DB_HOST     = os.getenv("postgres_host", "localhost")
DB_PORT     = os.getenv("postgres_port", "5432")
DB_NAME     = os.getenv("postgres_db", "market_data")
DB_USER     = os.getenv("postgres_user", "marketpulse")
DB_PASSWORD = os.getenv("postgres_password", "mp_secret_2026")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT") or os.getenv("minio_endpoint") or "localhost:9005"
MINIO_USER     = os.getenv("minio_root_user") or os.getenv("MINIO_ROOT_USER") or "minioadmin"
MINIO_PASSWORD = os.getenv("minio_root_password") or os.getenv("MINIO_ROOT_PASSWORD") or "minioadmin"

DELTA_TABLE_URI = "s3://market-data/bronze/ohlc"

API_RETRY_MAX   = 3
API_RETRY_DELAY = 2.0
API_PAGE_DELAY  = 0.1  # Delay between requests to avoid rate limits

ICT_TZ = timezone(timedelta(hours=7))

# ── PyArrow Schema ────────────────────────────────────────────────
ARROW_SCHEMA = pa.schema([
    ("symbol",     pa.string()),
    ("resolution", pa.string()),
    ("open",       pa.float64()),
    ("high",       pa.float64()),
    ("low",        pa.float64()),
    ("close",      pa.float64()),
    ("volume",     pa.int64()),
    ("time",       pa.timestamp("s", tz="UTC")),
    ("date",       pa.string()),
])

# ── Storage Options for Delta Lake ────────────────────────────────
def _build_storage_options() -> dict:
    return {
        "AWS_ENDPOINT_URL":          f"http://{MINIO_ENDPOINT}",
        "AWS_ACCESS_KEY_ID":         MINIO_USER,
        "AWS_SECRET_ACCESS_KEY":     MINIO_PASSWORD,
        "AWS_REGION":                "us-east-1",
        "AWS_ALLOW_HTTP":            "true",
        "AWS_FORCE_PATH_STYLE":      "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME":"true",
    }

# ── Helpers ───────────────────────────────────────────────────────
def _create_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        connect_timeout=5,
    )


def _ensure_bucket_exists():
    """Ensure bucket exists on MinIO."""
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        region_name="us-east-1",
    )
    try:
        s3.head_bucket(Bucket="market-data")
    except ClientError:
        try:
            s3.create_bucket(Bucket="market-data")
            print("[MINIO] Created bucket: market-data")
        except ClientError as e:
            error_code = e.response["Error"].get("Code", "")
            if error_code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise


def _parse_ts(ts_str: str) -> datetime:
    """Parse time string in ICT or ISO 8601 to datetime (UTC)."""
    ts_str = ts_str.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"):
        try:
            return datetime.strptime(ts_str, fmt).astimezone(timezone.utc)
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            naive = datetime.strptime(ts_str, fmt)
            return naive.replace(tzinfo=ICT_TZ).astimezone(timezone.utc)
        except ValueError:
            pass
    raise ValueError(f"Cannot parse timestamp: '{ts_str}'")


def get_bar_type(symbol: str) -> str:
    """Classify bar_type for DNSE API."""
    sym = symbol.upper().strip()
    indices = {"VN30", "VNINDEX", "HNX", "HNX30", "VN100", "UPCOM"}
    if sym in indices:
        return "INDEX"
    if (sym.startswith("VN30F") or 
        sym.startswith("V100F") or 
        sym.startswith("41I1") or 
        sym.startswith("41I2")):
        return "DERIVATIVE"
    return "STOCK"


def normalize_resolution(resolution: str) -> str:
    """Normalize resolution parameter to standard values."""
    res_map = {
        "1": "1min", "1min": "1min",
        "3": "3min", "3min": "3min",
        "5": "5min", "5min": "5min",
        "15": "15min", "15min": "15min",
        "30": "30min", "30min": "30min",
        "1h": "1H", "1H": "1H", "60": "1H",
        "1d": "1D", "1D": "1D", "d": "1D", "D": "1D",
        "1w": "1W", "1W": "1W", "w": "1W", "W": "1W"
    }
    norm = res_map.get(resolution.strip())
    if not norm:
        raise ValueError(f"Invalid resolution: '{resolution}'")
    return norm


def get_time_chunks(from_ts: datetime, to_ts: datetime, resolution: str) -> list[tuple[datetime, datetime]]:
    """Chunk query range to avoid API timeouts/limits."""
    if resolution in ("1min", "3min", "5min", "15min", "30min"):
        delta = timedelta(days=5)
    elif resolution == "1H":
        delta = timedelta(days=30)
    else:
        delta = timedelta(days=365)

    chunks = []
    curr = from_ts
    while curr < to_ts:
        end = min(curr + delta, to_ts)
        chunks.append((curr, end))
        curr = end
    return chunks


def _api_request_with_retry(client: DNSEClient, bar_type: str, query: dict) -> tuple[int, Any]:
    """Call get_ohlc API with exponential backoff retries."""
    for attempt in range(1, API_RETRY_MAX + 1):
        try:
            status, body = client.get_ohlc(bar_type=bar_type, query=query, dry_run=False)
            if status == 200:
                return status, body
            print(f"  [WARN] HTTP {status} (attempt {attempt}/{API_RETRY_MAX}): {body!r:.120}")
        except Exception as e:
            print(f"  [WARN] API Exception (attempt {attempt}/{API_RETRY_MAX}): {e}")
        
        if attempt < API_RETRY_MAX:
            time.sleep(API_RETRY_DELAY * attempt)
    return 500, "API failed after max retries"


# ── Core Ingestion Logic ──────────────────────────────────────────
def ingest_ohlc_chunk(
    client: DNSEClient,
    symbol: str,
    resolution: str,
    from_ts: datetime,
    to_ts: datetime,
    overwrite: bool = False,
) -> dict:
    """
    Fetch OHLC for range in chunks and write to Delta Table.
    """
    stats = {"fetched": 0, "inserted": 0, "skipped_dup": 0, "chunks": 0}
    
    bar_type = get_bar_type(symbol)
    norm_res = normalize_resolution(resolution)
    
    if bar_type == "DERIVATIVE" and symbol.upper().startswith("41I"):
        print(f"  [WARN] Symbol '{symbol}' is a physical derivative code. DNSE OHLC REST API usually fails or returns empty array. "
              f"Please consider using rolling codes (e.g. VN30F1M).")

    chunks = get_time_chunks(from_ts, to_ts, norm_res)
    stats["chunks"] = len(chunks)
    
    storage_options = _build_storage_options()
    _ensure_bucket_exists()

    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        region_name="us-east-1",
    )
    ckpt_mgr = CheckpointManager(s3_client, bucket="market-data", data_type="ohlc")

    if overwrite:
        ckpt_mgr.clear_checkpoints_for_range(symbol, norm_res, from_ts, to_ts)
        try:
            dt = DeltaTable(DELTA_TABLE_URI, storage_options=storage_options)
            from_date = (from_ts + timedelta(hours=7)).date().isoformat()
            to_date = (to_ts + timedelta(hours=7)).date().isoformat()
            predicate = f"symbol = '{symbol}' AND resolution = '{norm_res}' AND date >= '{from_date}' AND date <= '{to_date}'"
            print(f"  [OVERWRITE] Deleting existing data on Delta Lake: {predicate}")
            dt.delete(predicate=predicate)
        except TableNotFoundError:
            pass
        except Exception as e:
            print(f"  [WARN] Could not delete old data prior to overwrite: {e}")

    for idx, (chunk_start, chunk_end) in enumerate(chunks, 1):
        ckpt_key = ckpt_mgr.get_ohlc_key(symbol, norm_res, chunk_start, chunk_end)
        if not overwrite and ckpt_mgr.checkpoint_exists(ckpt_key):
            print(f"  [CHUNK {idx}/{len(chunks)}] Checkpoint exists. Skipping.")
            try:
                meta = ckpt_mgr.read_checkpoint(ckpt_key)
                stats["fetched"] += meta.get("records_written", 0)
                stats["inserted"] += meta.get("records_written", 0)
            except Exception:
                pass
            continue

        print(f"  [CHUNK {idx}/{len(chunks)}] Requesting: {chunk_start.isoformat()} -> {chunk_end.isoformat()} (UTC)")
        
        api_res = norm_res.replace("min", "") if norm_res.endswith("min") else norm_res
        status, body = _api_request_with_retry(
            client,
            bar_type=bar_type,
            query={
                "symbol": symbol,
                "resolution": api_res,
                "from": int(chunk_start.timestamp()) - 60,
                "to": int(chunk_end.timestamp()),
            }
        )

        if status != 200:
            print(f"  [ERROR] API Error during chunk fetch. Stopping.")
            break

        if isinstance(body, str):
            data = json.loads(body)
        else:
            data = body or {}

        t_list = data.get("t", [])
        if not t_list:
            print("  [CHUNK] No data returned.")
            # Write empty checkpoint to avoid requesting this range again
            ckpt_mgr.write_checkpoint(ckpt_key, {
                "symbol": symbol,
                "resolution": norm_res,
                "chunk_start": chunk_start.isoformat(),
                "chunk_end": chunk_end.isoformat(),
                "records_written": 0,
                "api_calls": 1,
            })
            continue

        stats["fetched"] += len(t_list)

        rows = []
        for i in range(len(t_list)):
            epoch_sec = t_list[i]
            dt_utc = datetime.fromtimestamp(epoch_sec, tz=timezone.utc)
            dt_ict = dt_utc.astimezone(ICT_TZ)
            date_str = dt_ict.strftime("%Y-%m-%d")

            rows.append({
                "symbol":     symbol,
                "resolution": norm_res,
                "open":       float(data["o"][i]),
                "high":       float(data["h"][i]),
                "low":        float(data["l"][i]),
                "close":      float(data["c"][i]),
                "volume":     int(data["v"][i]),
                "time":       dt_utc,
                "date":       date_str,
            })

        if not overwrite:
            try:
                dt = DeltaTable(DELTA_TABLE_URI, storage_options=storage_options)
                chunk_start_date = (chunk_start + timedelta(hours=7)).date().isoformat()
                chunk_end_date   = (chunk_end   + timedelta(hours=7)).date().isoformat()
                existing_table = dt.to_pyarrow_table(
                    columns=["time"],
                    partitions=[
                        ("symbol",     "=", symbol),
                        ("resolution", "=", norm_res),
                    ],
                    filters=[
                        ("date", ">=", chunk_start_date),
                        ("date", "<=", chunk_end_date),
                    ]
                )
                existing_times = set(existing_table.column("time").to_pylist())
            except TableNotFoundError:
                existing_times = set()
            except Exception as e:
                print(f"  [WARN] Failed to read existing partition times for deduplication: {e}")
                existing_times = set()

            filtered_rows = []
            for r in rows:
                if r["time"] not in existing_times:
                    filtered_rows.append(r)
                else:
                    stats["skipped_dup"] += 1
        else:
            filtered_rows = rows

        if filtered_rows:
            df = pd.DataFrame(filtered_rows)
            table = pa.Table.from_pandas(df, schema=ARROW_SCHEMA)
            
            write_deltalake(
                DELTA_TABLE_URI,
                table,
                mode="append",
                partition_by=["symbol", "resolution"],
                storage_options=storage_options,
                schema_mode="merge",
            )
            stats["inserted"] += len(filtered_rows)
            print(f"  [INSERT] +{len(filtered_rows)} rows to Delta Lake | skip_dup={stats['skipped_dup']}")
            
            ckpt_mgr.write_checkpoint(ckpt_key, {
                "symbol": symbol,
                "resolution": norm_res,
                "chunk_start": chunk_start.isoformat(),
                "chunk_end": chunk_end.isoformat(),
                "records_written": len(filtered_rows),
                "api_calls": 1,
            })
        else:
            print("  [CHUNK] No new rows to write (all are duplicates).")
            ckpt_mgr.write_checkpoint(ckpt_key, {
                "symbol": symbol,
                "resolution": norm_res,
                "chunk_start": chunk_start.isoformat(),
                "chunk_end": chunk_end.isoformat(),
                "records_written": 0,
                "api_calls": 1,
            })

        if idx < len(chunks):
            time.sleep(API_PAGE_DELAY)

    return stats


# ── Public APIs ───────────────────────────────────────────────────
def run_backfill_ohlc(
    symbol: str,
    resolution: str,
    from_ts_str: str,
    to_ts_str: str,
    overwrite: bool = False,
) -> dict:
    """
    Case 1: Manual historical backfill for backtesting.
    """
    sym_upper = symbol.upper().strip()
    print(f"\n=== STARTING MANUAL OHLC BACKFILL: {sym_upper} (res={resolution}) ===")
    from_ts = _parse_ts(from_ts_str)
    to_ts   = _parse_ts(to_ts_str)

    if from_ts >= to_ts:
        raise ValueError(f"from_ts ({from_ts_str}) must be less than to_ts ({to_ts_str})")

    client = DNSEClient(
        api_key=DNSE_API_KEY,
        api_secret=DNSE_API_SECRET,
        base_url=DNSE_BASE_URL,
    )

    # Check if symbol is an index name to expand it to constituents
    target_symbols = [sym_upper]
    if sym_upper in ("VN30", "VN100", "HNX30"):
        print(f"  [INDEX GROUP] Resolving constituent symbols for index: {sym_upper}")
        try:
            conn = _create_db_conn()
            with conn.cursor() as cur:
                index_pattern = f"(^|,){sym_upper}(,|$)"
                cur.execute(
                    "SELECT DISTINCT symbol FROM instrument_master WHERE is_active = true AND index_name ~ %s ORDER BY symbol",
                    (index_pattern,)
                )
                constituents = [r[0] for r in cur.fetchall()]
            conn.close()
            if constituents:
                print(f"  [INDEX GROUP] Resolved {len(constituents)} constituent symbols.")
                target_symbols = constituents
        except Exception as e:
            print(f"  [WARN] Failed to resolve index constituents: {e}. Using '{sym_upper}' as index itself.")

    total_stats = {"fetched": 0, "inserted": 0, "skipped_dup": 0, "chunks": 0}

    for idx, tsym in enumerate(target_symbols, 1):
        if len(target_symbols) > 1:
            print(f"\n[{idx}/{len(target_symbols)}] Backfilling constituent {tsym}...")
        
        stats = ingest_ohlc_chunk(
            client=client,
            symbol=tsym,
            resolution=resolution,
            from_ts=from_ts,
            to_ts=to_ts,
            overwrite=overwrite,
        )
        total_stats["fetched"] += stats["fetched"]
        total_stats["inserted"] += stats["inserted"]
        total_stats["skipped_dup"] += stats["skipped_dup"]
        total_stats["chunks"] += stats["chunks"]
        
        if len(target_symbols) > 1 and idx < len(target_symbols):
            time.sleep(API_PAGE_DELAY)

    # ── Post-Backfill Verification ───────────────────────────────────
    print("\n=== POST-BACKFILL VERIFICATION ===")
    storage_options = _build_storage_options()
    for tsym in target_symbols:
        try:
            dt = DeltaTable(DELTA_TABLE_URI, storage_options=storage_options)
            tbl = dt.to_pyarrow_table(
                columns=["time"],
                partitions=[("symbol", "=", tsym), ("resolution", "=", resolution)]
            )
            if len(tbl) > 0:
                df_ts = tbl.column("time").to_pandas()
                min_t = df_ts.min().astimezone(ICT_TZ)
                max_t = df_ts.max().astimezone(ICT_TZ)
                print(f"  [VERIFY] {tsym} (res={resolution}) Delta Lake: {len(tbl)} total rows | Range: {min_t.strftime('%Y-%m-%d %H:%M:%S')} -> {max_t.strftime('%Y-%m-%d %H:%M:%S')} (ICT)")
            else:
                print(f"  [VERIFY] {tsym} (res={resolution}) Delta Lake has 0 rows.")
        except TableNotFoundError:
            print(f"  [VERIFY] {tsym} (res={resolution}) Table not found in Delta Lake.")
        except Exception as e:
            print(f"  [VERIFY] {tsym} (res={resolution}) Verification error: {e}")

    print(f"\n=== BACKFILL COMPLETE: symbols={len(target_symbols)} fetched={total_stats['fetched']} inserted={total_stats['inserted']} skip_dup={total_stats['skipped_dup']} ===\n")
    return total_stats


def run_eod_ohlc(
    date_str: str | None = None,
    resolutions: list[str] | None = None,
    overwrite: bool = False,
) -> dict:
    """
    Case 2: End-of-day task calling REST API to retrieve full day's OHLC for all resolved symbols.
    Defaults to resolutions: ["1", "1D"]
    """
    if not date_str:
        date_str = (datetime.now(timezone.utc) + timedelta(hours=7)).date().isoformat()
    
    if not resolutions:
        resolutions = ["1", "1D"]

    print(f"\n=== STARTING EOD OHLC REST API ARCHIVE FOR DATE: {date_str} ===")
    
    conn = None
    try:
        conn = _create_db_conn()
    except Exception as e:
        print(f"[ERROR] Cannot connect to TimescaleDB to fetch symbols: {e}")
        return {}

    possible_roots = [
        Path(__file__).resolve().parents[2],  # Host path
        Path("/opt/airflow"),                  # Container path
    ]
    for root in possible_roots:
        if root.exists() and str(root) not in sys.path:
            sys.path.insert(0, str(root))

    # Retrieve symbol filters config
    try:
        from ingestion.common.symbol_resolver import SymbolResolver
        resolver = SymbolResolver()
        cfg = resolver._build_filter_config()
    except Exception as e:
        print(f"[WARN] Failed to read SymbolResolver config: {e}. Using empty filters.")
        cfg = {"groups": [], "indexes": [], "markets": []}

    # Query active symbols directly from instrument_master (no join with security_definition)
    where = ["is_active = true"]
    params = []
    
    selection_clauses = []
    if cfg.get("groups"):
        selection_clauses.append("security_group_id = ANY(%s)")
        params.append(cfg["groups"])
    if cfg.get("indexes"):
        index_pattern = "|".join(f"(^|,){idx}(,|$)" for idx in cfg["indexes"])
        selection_clauses.append("index_name ~ %s")
        params.append(index_pattern)
        
    if selection_clauses:
        where.append(f"({' OR '.join(selection_clauses)})")
        
    if cfg.get("markets"):
        where.append("market_id = ANY(%s)")
        params.append(cfg["markets"])
        
    sql = f"""
        SELECT DISTINCT symbol
        FROM instrument_master
        WHERE {' AND '.join(where)}
        ORDER BY symbol
    """
    
    symbols = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            symbols = [r[0] for r in cur.fetchall()]
        print(f"[DB] Found {len(symbols)} active symbols in instrument_master.")
    except Exception as e:
        print(f"[ERROR] Error querying instrument_master: {e}")
        return {}
    finally:
        conn.close()

    if not symbols:
        print("[WARN] Active symbols list is empty. Skipping EOD execution.")
        return {}

    # Skip physical derivatives (starting with 41I)
    filtered_symbols = [s for s in symbols if not s.startswith("41I")]
    
    # Add rolling derivatives
    rolling_derivatives = ["VN30F1M", "VN30F2M", "VN30F1Q", "VN30F2Q"]
    for rd in rolling_derivatives:
        if rd not in filtered_symbols:
            filtered_symbols.append(rd)
            
    print(f"[RESOLVE] Processed symbols list (skipped physical, added rolling): {len(filtered_symbols)} symbols.")

    # Calculate full-day query range in ICT (00:00:00 to 23:59:59)
    ict_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    from_ts = datetime(ict_date.year, ict_date.month, ict_date.day, 0, 0, 0, tzinfo=ICT_TZ).astimezone(timezone.utc)
    to_ts   = datetime(ict_date.year, ict_date.month, ict_date.day, 23, 59, 59, tzinfo=ICT_TZ).astimezone(timezone.utc)

    client = DNSEClient(
        api_key=DNSE_API_KEY,
        api_secret=DNSE_API_SECRET,
        base_url=DNSE_BASE_URL,
    )

    results = {}
    
    for resolution in resolutions:
        norm_res = normalize_resolution(resolution)
        print(f"\n--- Starting EOD Archiving for Resolution: {norm_res} ---")
        results[norm_res] = {"fetched": 0, "inserted": 0, "skipped_dup": 0, "failed_symbols": []}
        
        for idx, symbol in enumerate(filtered_symbols, 1):
            print(f"[{idx}/{len(filtered_symbols)}] Processing {symbol}...")
            try:
                stats = ingest_ohlc_chunk(
                    client=client,
                    symbol=symbol,
                    resolution=norm_res,
                    from_ts=from_ts,
                    to_ts=to_ts,
                    overwrite=overwrite,
                )
                results[norm_res]["fetched"] += stats["fetched"]
                results[norm_res]["inserted"] += stats["inserted"]
                results[norm_res]["skipped_dup"] += stats["skipped_dup"]
            except Exception as e:
                print(f"  [ERROR] Failed to process ohlc for {symbol}: {e}")
                results[norm_res]["failed_symbols"].append(symbol)
                
            time.sleep(API_PAGE_DELAY)

    print(f"\n=== EOD REST API ARCHIVE COMPLETE FOR DATE: {date_str} ===")
    for res, stats in results.items():
        print(f"  Resolution {res}: fetched={stats['fetched']} | inserted={stats['inserted']} | skip_dup={stats['skipped_dup']} | failed={len(stats['failed_symbols'])}")
        if stats["failed_symbols"]:
            print(f"    Failed symbols: {stats['failed_symbols']}")
            
    return results


# ── Entry Point CLI ───────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill or save EOD OHLC data from DNSE REST API to Delta Lake.")
    parser.add_argument("--symbol", type=str, help="Symbol to backfill (e.g. VCB, VN30F1M)")
    parser.add_argument("--resolution", type=str, help="Resolution (e.g. 1, 1H, 1D, 1W)")
    parser.add_argument("--from", dest="from_ts", type=str, help="Start time ICT (e.g. '2026-06-01 09:00:00')")
    parser.add_argument("--to", dest="to_ts", type=str, help="End time ICT (e.g. '2026-06-23 15:00:00')")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing data range instead of skipping duplicates")
    parser.add_argument("--eod", action="store_true", help="Run in end-of-day mode for active symbols")
    parser.add_argument("--date", type=str, help="EOD date in YYYY-MM-DD format (only valid with --eod)")

    args = parser.parse_args()

    if args.eod:
        run_eod_ohlc(date_str=args.date, overwrite=args.overwrite)
    else:
        if not args.symbol or not args.resolution or not args.from_ts or not args.to_ts:
            parser.print_help()
            sys.exit(1)
        run_backfill_ohlc(
            symbol=args.symbol,
            resolution=args.resolution,
            from_ts_str=args.from_ts,
            to_ts_str=args.to_ts,
            overwrite=args.overwrite,
        )
