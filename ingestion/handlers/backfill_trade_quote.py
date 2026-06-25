"""
ingestion/handlers/backfill_trade_quote.py

Backfill historical market data from DNSE REST API (trade, quote) into TimescaleDB.

Can be invoked in two ways:
  1. Direct CLI invocation (dev/debug):
       python ingestion/handlers/backfill_trade_quote.py \
         --symbol VCB --type trade \
         --from "2026-06-23 10:00:00" --to "2026-06-23 10:15:00" \
         --target db

  2. Airflow DAG import (production):
       from ingestion.handlers.backfill_trade_quote import run_backfill
       run_backfill("VCB", "trade", "2026-06-23 10:00:00", "2026-06-23 10:15:00", "db")

Retention windows (from 01_schema.sql):
  - market_trade:  30 days -> only write to DB if from_ts >= today - 30d
  - order_book_l2:  7 days -> only write to DB if from_ts >= today - 7d

Deduplication:
  - Trade:  fingerprint = (symbol, time_str, price, quantity, side)
  - Quote:  fingerprint = (symbol, time_str, bid_price1, ask_price1)
  Prior to INSERT, fetch existing fingerprints from DB in range [from_ts, to_ts],
  and filter out matching records.

API pagination:
  - limit=1000 per page (DNSE maximum limit)
  - Automatically paginate using nextPageToken until exhaustion.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
from botocore.exceptions import ClientError

# ── SDK path setup ────────────────────────────────────────────────
# Works in local dev and Docker/Airflow (mounted at /opt/airflow/sdk)
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

# ── Config ────────────────────────────────────────────────────────
DNSE_API_KEY    = os.environ.get("DNSE_API_KEY", "")
DNSE_API_SECRET = os.environ.get("DNSE_API_SECRET", "")
DNSE_BASE_URL   = "https://openapi.dnse.com.vn"

DB_HOST     = os.getenv("postgres_host", "localhost")
DB_PORT     = os.getenv("postgres_port", "5432")
DB_NAME     = os.getenv("postgres_db", "market_data")
DB_USER     = os.getenv("postgres_user", "marketpulse")
DB_PASSWORD = os.getenv("postgres_password", "mp_secret_2026")

API_LIMIT         = 1000           # max page size of DNSE API
API_PAGE_DELAY    = 0.2            # seconds delay to avoid rate limit
API_RETRY_MAX     = 3
API_RETRY_DELAY   = 2.0

# Retention windows (must match 01_schema.sql)
RETENTION_TRADE_DAYS = 30
RETENTION_QUOTE_DAYS = 7

# ── MinIO & Delta Lake Configs ────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT") or os.getenv("minio_endpoint") or "localhost:9005"
MINIO_USER     = os.getenv("minio_root_user") or os.getenv("MINIO_ROOT_USER") or "minioadmin"
MINIO_PASSWORD = os.getenv("minio_root_password") or os.getenv("MINIO_ROOT_PASSWORD") or "minioadmin"

TRADE_DELTA_TABLE_URI = "s3://market-data/bronze/market_trade"
QUOTE_DELTA_TABLE_URI = "s3://market-data/bronze/market_quote"

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

def _ensure_bucket_exists():
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

TRADE_ARROW_SCHEMA = pa.schema([
    ("symbol",          pa.string()),
    ("market_id",       pa.string()),
    ("board_id",        pa.string()),
    ("price",           pa.float64()),
    ("quantity",        pa.int32()),
    ("side",            pa.int32()),
    ("session_vol",     pa.int64()),
    ("session_high",    pa.float64()),
    ("session_low",     pa.float64()),
    ("session_open",    pa.float64()),
    ("session_vwap",    pa.float64()),
    ("trading_session_id", pa.string()),
    ("exchange_ts",     pa.timestamp("ms", tz="UTC")),
    ("dnse_ts",         pa.timestamp("ms", tz="UTC")),
    ("producer_ts",     pa.timestamp("ms", tz="UTC")),
    ("date",            pa.string()),
    ("kafka_partition", pa.int32()),
    ("kafka_offset",    pa.int64()),
])

QUOTE_ARROW_SCHEMA = pa.schema([
    ("symbol",        pa.string()),
    ("market_id",     pa.string()),
    ("board_id",      pa.string()),
    ("bid_price1",    pa.float64()),
    ("bid_qty1",      pa.int32()),
    ("bid_price2",    pa.float64()),
    ("bid_qty2",      pa.int32()),
    ("bid_price3",    pa.float64()),
    ("bid_qty3",      pa.int32()),
    ("ask_price1",    pa.float64()),
    ("ask_qty1",      pa.int32()),
    ("ask_price2",    pa.float64()),
    ("ask_qty2",      pa.int32()),
    ("ask_price3",    pa.float64()),
    ("ask_qty3",      pa.int32()),
    ("total_bid_qty", pa.int64()),
    ("total_ask_qty", pa.int64()),
    ("bid_levels", pa.list_(pa.struct([
        pa.field("price", pa.float64()),
        pa.field("qtty",  pa.int32()),
    ]))),
    ("ask_levels", pa.list_(pa.struct([
        pa.field("price", pa.float64()),
        pa.field("qtty",  pa.int32()),
    ]))),
    ("exchange_ts",   pa.timestamp("ms", tz="UTC")),
    ("dnse_ts",       pa.timestamp("ms", tz="UTC")),
    ("producer_ts",   pa.timestamp("ms", tz="UTC")),
    ("date",            pa.string()),
    ("kafka_partition", pa.int32()),
    ("kafka_offset",    pa.int64()),
])

# ── DB helpers ────────────────────────────────────────────────────
def _create_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    )


def _parse_ts(ts_str: str) -> datetime:
    """
    Convert string timestamp to datetime (UTC).
    Supports:
      - "2026-06-23 10:00:00"  (assumes ICT timezone, converts to UTC -7h)
      - ISO 8601: "2026-06-23T10:00:00+07:00"
    """
    ts_str = ts_str.strip()
    # Try parsing ISO 8601 first
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"):
        try:
            return datetime.strptime(ts_str, fmt).astimezone(timezone.utc)
        except ValueError:
            pass
    # Naive string -> assume ICT (UTC+7)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            naive = datetime.strptime(ts_str, fmt)
            ict = timezone(timedelta(hours=7))
            return naive.replace(tzinfo=ict).astimezone(timezone.utc)
        except ValueError:
            pass
    raise ValueError(
        f"Cannot parse timestamp: '{ts_str}'. "
        "Use format: 'YYYY-MM-DD HH:MM:SS' (ICT) or ISO 8601."
    )


def _to_epoch(dt: datetime) -> int:
    """datetime -> Unix timestamp (seconds)."""
    return int(dt.timestamp())


def _check_retention(from_ts: datetime, table: str, days: int) -> bool:
    """
    Check if from_ts falls within the retention window of the target table.
    Returns True if valid to write to DB, False if expired.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return from_ts >= cutoff


# ── API helpers ───────────────────────────────────────────────────
def _api_request_with_retry(func, *args, **kwargs) -> tuple[int, Any]:
    """Call func(*args, **kwargs) with retries up to API_RETRY_MAX times."""
    status, body = 500, "API request failed"
    for attempt in range(1, API_RETRY_MAX + 1):
        try:
            status, body = func(*args, **kwargs)
            if status == 200:
                return status, body
            print(f"  [WARN] HTTP {status} (attempt {attempt}/{API_RETRY_MAX}): {body!r:.120}")
        except Exception as e:
            status, body = 500, f"Connection error: {e}"
            print(f"  [WARN] API Connection Exception (attempt {attempt}/{API_RETRY_MAX}): {e}")
        
        if attempt < API_RETRY_MAX:
            time.sleep(API_RETRY_DELAY * attempt)
    return status, body


def _parse_body(body: str | dict) -> dict:
    if isinstance(body, str):
        return json.loads(body)
    return body or {}


# ── Trade backfill ────────────────────────────────────────────────
_TRADE_INSERT_SQL = """
INSERT INTO market_trade (
    symbol, market_id, board_id,
    price, quantity, side,
    session_vol, session_high, session_low, session_open, session_vwap,
    exchange_ts, is_backfill
) VALUES %s
"""

def _fetch_existing_trade_fps(conn, symbol: str, from_ts: datetime, to_ts: datetime) -> set:
    """
    Fetch fingerprints of existing trades in DB in range [from_ts, to_ts].
    fingerprint = (symbol, exchange_ts_ms_truncated, price_rounded, quantity, side)

    Note: DB stores timestamps with microsecond precision, while DNSE REST API only
    has millisecond precision. Truncate using date_trunc('milliseconds') to align.
    Round price to 4 decimals to avoid float precision mismatches.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol,
                   (extract(epoch from date_trunc('milliseconds', exchange_ts)) * 1000)::bigint,
                   round(price::numeric, 4),
                   quantity,
                   side
            FROM market_trade
            WHERE symbol = %s
              AND exchange_ts >= %s
              AND exchange_ts <= %s
            """,
            (symbol, from_ts, to_ts),
        )
        rows = cur.fetchall()
    return {(r[0], int(r[1]), float(r[2]), r[3], r[4]) for r in rows}


def _to_ms_precision(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    ms = dt.microsecond // 1000 * 1000
    return dt.replace(microsecond=ms)


def _fetch_existing_trade_fps_delta(symbol: str, from_ts: datetime, to_ts: datetime, storage_options: dict) -> set:
    try:
        dt = DeltaTable(TRADE_DELTA_TABLE_URI, storage_options=storage_options)
        tbl = dt.to_pyarrow_table(
            columns=["symbol", "exchange_ts", "price", "quantity", "side"],
            partitions=[("symbol", "=", symbol)]
        )
        df = tbl.to_pandas()
        if df.empty:
            return set()
        # Filter by exchange_ts range
        mask = (df["exchange_ts"] >= from_ts) & (df["exchange_ts"] <= to_ts)
        filtered_df = df[mask]
        
        fps = set()
        for _, row in filtered_df.iterrows():
            ts_ms = int(row["exchange_ts"].timestamp() * 1000)
            price_rounded = round(float(row["price"]), 4)
            fps.add((row["symbol"], ts_ms, price_rounded, int(row["quantity"]), int(row["side"])))
        return fps
    except TableNotFoundError:
        return set()
    except Exception as e:
        print(f"  [WARN] Failed to read existing Delta Table fingerprints: {e}")
        return set()


def _trade_record_to_row_delta(t: dict, symbol: str) -> dict | None:
    try:
        time_str = t.get("time", "")
        if not time_str:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                ict = timezone(timedelta(hours=7))
                naive = datetime.strptime(time_str, fmt)
                exchange_ts = naive.replace(tzinfo=ict).astimezone(timezone.utc)
                break
            except ValueError:
                pass
        else:
            return None

        side_str = t.get("side", "UNSPECIFIED")
        side_map = {"BUY": 1, "SELL": 2, "UNSPECIFIED": 0}
        side = side_map.get(side_str.upper(), 0)

        ict_ts = exchange_ts.astimezone(timezone(timedelta(hours=7)))
        date_str = ict_ts.strftime("%Y-%m-%d")

        return {
            "symbol":          t.get("symbol", symbol),
            "market_id":       t.get("marketId", ""),
            "board_id":        t.get("boardId", "G1"),
            "price":           float(t.get("matchPrice", 0)),
            "quantity":        int(t.get("matchQtty", 0)),
            "side":            side,
            "session_vol":     int(t["totalVolumeTraded"]) if t.get("totalVolumeTraded") is not None else None,
            "session_high":    float(t["highestPrice"]) if t.get("highestPrice") is not None else None,
            "session_low":     float(t["lowestPrice"]) if t.get("lowestPrice") is not None else None,
            "session_open":    float(t["openPrice"]) if t.get("openPrice") is not None else None,
            "session_vwap":    float(t["avgPrice"]) if t.get("avgPrice") is not None else None,
            "trading_session_id": t.get("tradingSessionId"),
            "exchange_ts":     _to_ms_precision(exchange_ts),
            "dnse_ts":         None,
            "producer_ts":     _to_ms_precision(datetime.now(timezone.utc)),
            "date":            date_str,
            "kafka_partition": None,
            "kafka_offset":    None,
        }
    except Exception as e:
        print(f"  [WARN] Skip invalid trade delta record: {e} | data={t!r:.120}")
        return None


def _trade_record_to_row(t: dict, symbol: str) -> tuple | None:
    """Map trade API record to DB insert tuple."""
    try:
        # time: "2026-03-12 11:29:35.784" (ICT)
        time_str = t.get("time", "")
        if not time_str:
            return None
        # Parse ICT time
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                ict = timezone(timedelta(hours=7))
                naive = datetime.strptime(time_str, fmt)
                exchange_ts = naive.replace(tzinfo=ict).astimezone(timezone.utc)
                break
            except ValueError:
                pass
        else:
            return None

        side_str = t.get("side", "UNSPECIFIED")
        # DNSE REST API returns side as string BUY/SELL/UNSPECIFIED
        # Map to int to match Websocket: 1=buy, 2=sell, 0=unknown
        side_map = {"BUY": 1, "SELL": 2, "UNSPECIFIED": 0}
        side = side_map.get(side_str.upper(), 0)

        return (
            t.get("symbol", symbol),
            t.get("marketId", ""),
            t.get("boardId", "G1"),
            float(t.get("matchPrice", 0)),
            int(t.get("matchQtty", 0)),
            side,
            t.get("totalVolumeTraded"),
            t.get("highestPrice"),
            t.get("lowestPrice"),
            t.get("openPrice"),
            t.get("avgPrice"),
            exchange_ts,
            True,  # is_backfill
        )
    except Exception as e:
        print(f"  [WARN] Skip invalid trade record: {e} | data={t!r:.120}")
        return None


def _trade_fingerprint(row: tuple) -> tuple:
    """
    Get fingerprint from row tuple: (symbol, exchange_ts_ms, price_rounded, quantity, side).
    Aligns with DB fetch: ms-truncated timestamp + round(price, 4).
    """
    # row[11] = exchange_ts (datetime), row[3]=price, row[4]=quantity, row[5]=side
    exchange_ts_ms = int(row[11].timestamp() * 1000)  # ms precision
    price_rounded  = round(float(row[3]), 4)
    return (row[0], exchange_ts_ms, price_rounded, row[4], row[5])


def backfill_trade(
    client: DNSEClient,
    conn,
    symbol: str,
    from_ts: datetime,
    to_ts: datetime,
    write_db: bool,
    write_minio: bool = False,
    overwrite: bool = False,
) -> dict:
    """
    Backfill trade data for a symbol.
    Returns stats: {fetched, skipped_dup, inserted, pages}.
    """
    stats = {"fetched": 0, "skipped_dup": 0, "inserted": 0, "pages": 0}
    storage_options = _build_storage_options()

    if write_minio:
        _ensure_bucket_exists()

    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        region_name="us-east-1",
    )
    ckpt_mgr = CheckpointManager(s3_client, bucket="market-data", data_type="trade")

    if overwrite:
        ckpt_mgr.clear_checkpoints_for_range(symbol, None, from_ts, to_ts)
        # DB Overwrite
        if write_db and conn is not None:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM market_trade WHERE symbol = %s AND exchange_ts >= %s AND exchange_ts <= %s",
                        (symbol, from_ts, to_ts)
                    )
                conn.commit()
                print(f"  [OVERWRITE-DB] Deleted existing records in DB for range [{from_ts.isoformat()}, {to_ts.isoformat()}]")
            except Exception as e:
                print(f"  [WARN] Failed to delete existing DB records: {e}")

        # MinIO Overwrite
        if write_minio:
            try:
                dt = DeltaTable(TRADE_DELTA_TABLE_URI, storage_options=storage_options)
                from_date = (from_ts + timedelta(hours=7)).date().isoformat()
                to_date = (to_ts + timedelta(hours=7)).date().isoformat()
                predicate = f"symbol = '{symbol}' AND date >= '{from_date}' AND date <= '{to_date}'"
                print(f"  [OVERWRITE-MINIO] Deleting existing data: {predicate}")
                dt.delete(predicate=predicate)
            except TableNotFoundError:
                pass
            except Exception as e:
                print(f"  [WARN] Failed to delete existing Delta Table records: {e}")

    # Check retention window
    if write_db and not _check_retention(from_ts, "market_trade", RETENTION_TRADE_DAYS):
        cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_TRADE_DAYS)
        print(
            f"  [WARN] from_ts={from_ts.isoformat()} exceeds retention limit of {RETENTION_TRADE_DAYS} days "
            f"(cutoff={cutoff.date()}). Skipping database insertion."
        )
        write_db = False

    # Check checkpoints
    last_page, resume_token = 0, None
    if not overwrite:
        last_page, resume_token = ckpt_mgr.get_last_history_page(symbol, from_ts, to_ts)
        if last_page > 0:
            if not resume_token:
                print(f"  [CHECKPOINT] Range [{from_ts.isoformat()}, {to_ts.isoformat()}] already completed. Skipping.")
                return stats
            print(f"  [CHECKPOINT] Resuming from page {last_page} (nextPageToken: {resume_token})")
            stats["pages"] = last_page

    # Fetch existing DB fingerprints for deduplication
    existing_fps: set = set()
    if write_db and not overwrite and conn is not None:
        print("  [DEDUP-DB] Fetching existing fingerprints from DB...")
        existing_fps = _fetch_existing_trade_fps(conn, symbol, from_ts, to_ts)
        print(f"  [DEDUP-DB] Found {len(existing_fps)} records in range [from, to]")

    # Fetch existing Delta fingerprints for deduplication
    existing_fps_delta: set = set()
    if write_minio and not overwrite:
        print("  [DEDUP-MINIO] Fetching existing fingerprints from Delta Lake...")
        existing_fps_delta = _fetch_existing_trade_fps_delta(symbol, from_ts, to_ts, storage_options)
        print(f"  [DEDUP-MINIO] Found {len(existing_fps_delta)} records in range [from, to]")

    from_epoch = _to_epoch(from_ts)
    to_epoch   = _to_epoch(to_ts)

    next_token = resume_token
    page_num = last_page + 1
    while True:
        status, body = _api_request_with_retry(
            client.get_trades,
            symbol=symbol,
            board_id="G1",
            from_date=from_epoch,
            to_date=to_epoch,
            limit=API_LIMIT,
            order="ASC",
            next_page_token=next_token,
        )

        if status != 200:
            print(f"  [ERROR] API returned HTTP {status}. Terminating pagination.")
            break

        data = _parse_body(body)
        trades = data.get("trades", [])
        stats["pages"] = page_num
        stats["fetched"] += len(trades)
        print(f"  [PAGE {page_num}] Received {len(trades)} trades")

        db_rows_written = 0
        minio_rows_written = 0

        if write_db and trades and conn is not None:
            rows_to_insert = []
            for t in trades:
                row = _trade_record_to_row(t, symbol)
                if row is None:
                    continue
                fp = _trade_fingerprint(row)
                if not overwrite and fp in existing_fps:
                    stats["skipped_dup"] += 1
                else:
                    rows_to_insert.append(row)
                    existing_fps.add(fp)

            if rows_to_insert:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur, _TRADE_INSERT_SQL, rows_to_insert,
                        template=None, page_size=len(rows_to_insert),
                    )
                conn.commit()
                stats["inserted"] += len(rows_to_insert)
                db_rows_written = len(rows_to_insert)
                print(f"  [INSERT-DB] +{len(rows_to_insert)} rows | skip_dup={stats['skipped_dup']}")

        if write_minio and trades:
            delta_rows = []
            skipped_dup_delta = 0
            for t in trades:
                row = _trade_record_to_row_delta(t, symbol)
                if row is None:
                    continue
                exchange_ts_ms = int(row["exchange_ts"].timestamp() * 1000)
                price_rounded = round(float(row["price"]), 4)
                fp = (row["symbol"], exchange_ts_ms, price_rounded, row["quantity"], row["side"])
                
                if not overwrite and fp in existing_fps_delta:
                    skipped_dup_delta += 1
                else:
                    delta_rows.append(row)
                    existing_fps_delta.add(fp)

            if delta_rows:
                df = pd.DataFrame(delta_rows)
                table = pa.Table.from_pandas(df, schema=TRADE_ARROW_SCHEMA)
                write_deltalake(
                    TRADE_DELTA_TABLE_URI,
                    table,
                    mode="append",
                    partition_by=["date"],
                    storage_options=storage_options,
                    schema_mode="merge",
                )
                minio_rows_written = len(delta_rows)
                print(f"  [INSERT-MINIO] +{len(delta_rows)} rows to Delta Lake | skipped_dup={skipped_dup_delta}")
            else:
                print("  [CHUNK] No new rows to write to Delta Lake (all are duplicates).")

        next_token_next = data.get("nextPageToken") or None
        
        # Write checkpoint for this page
        ckpt_key = ckpt_mgr.get_history_key(symbol, from_ts, to_ts, page_num)
        ckpt_mgr.write_checkpoint(ckpt_key, {
            "symbol": symbol,
            "chunk_start": from_ts.isoformat(),
            "chunk_end": to_ts.isoformat(),
            "page_num": page_num,
            "records_written": max(db_rows_written, minio_rows_written),
            "next_page_token": next_token_next,
        })

        next_token = next_token_next
        page_num += 1
        
        if not next_token or not trades:
            break
        time.sleep(API_PAGE_DELAY)

    return stats


# ── Quote backfill ────────────────────────────────────────────────
_QUOTE_INSERT_SQL = """
INSERT INTO order_book_l2 (
    symbol, market_id,
    bid_price1, bid_qty1,
    bid_price2, bid_qty2,
    bid_price3, bid_qty3,
    ask_price1, ask_qty1,
    ask_price2, ask_qty2,
    ask_price3, ask_qty3,
    exchange_ts, is_backfill
) VALUES %s
"""


def _fetch_existing_quote_fps(conn, symbol: str, from_ts: datetime, to_ts: datetime) -> set:
    """
    Fetch fingerprints of existing quotes in DB in range [from_ts, to_ts].
    fingerprint = (symbol, exchange_ts_ms_truncated, bid_price1_rounded, ask_price1_rounded)
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol,
                   (extract(epoch from date_trunc('milliseconds', exchange_ts)) * 1000)::bigint,
                   round(bid_price1::numeric, 4),
                   round(ask_price1::numeric, 4)
            FROM order_book_l2
            WHERE symbol = %s
              AND exchange_ts >= %s
              AND exchange_ts <= %s
            """,
            (symbol, from_ts, to_ts),
        )
        rows = cur.fetchall()
    return {(r[0], int(r[1]), float(r[2]) if r[2] is not None else None, float(r[3]) if r[3] is not None else None) for r in rows}


def _fetch_existing_quote_fps_delta(symbol: str, from_ts: datetime, to_ts: datetime, storage_options: dict) -> set:
    try:
        dt = DeltaTable(QUOTE_DELTA_TABLE_URI, storage_options=storage_options)
        tbl = dt.to_pyarrow_table(
            columns=["symbol", "exchange_ts", "bid_price1", "ask_price1"],
            partitions=[("symbol", "=", symbol)]
        )
        df = tbl.to_pandas()
        if df.empty:
            return set()
        # Filter by exchange_ts range
        mask = (df["exchange_ts"] >= from_ts) & (df["exchange_ts"] <= to_ts)
        filtered_df = df[mask]
        
        fps = set()
        for _, row in filtered_df.iterrows():
            ts_ms = int(row["exchange_ts"].timestamp() * 1000)
            bp1 = round(float(row["bid_price1"]), 4) if pd.notnull(row["bid_price1"]) else None
            ap1 = round(float(row["ask_price1"]), 4) if pd.notnull(row["ask_price1"]) else None
            fps.add((row["symbol"], ts_ms, bp1, ap1))
        return fps
    except TableNotFoundError:
        return set()
    except Exception as e:
        print(f"  [WARN] Failed to read existing Delta Table fingerprints: {e}")
        return set()


def _quote_record_to_row_delta(q: dict, symbol: str) -> dict | None:
    try:
        time_str = q.get("time", "")
        if not time_str:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                ict = timezone(timedelta(hours=7))
                naive = datetime.strptime(time_str, fmt)
                exchange_ts = naive.replace(tzinfo=ict).astimezone(timezone.utc)
                break
            except ValueError:
                pass
        else:
            return None

        bids = q.get("bid", [])
        offers = q.get("offer", [])
        
        bp1, bq1 = _parse_price_level(bids, 0)
        bp2, bq2 = _parse_price_level(bids, 1)
        bp3, bq3 = _parse_price_level(bids, 2)
        ap1, aq1 = _parse_price_level(offers, 0)
        ap2, aq2 = _parse_price_level(offers, 1)
        ap3, aq3 = _parse_price_level(offers, 2)
        
        bid_levels = [{"price": float(item.get("price", 0)), "qtty": int(item.get("quantity", 0))} for item in bids]
        ask_levels = [{"price": float(item.get("price", 0)), "qtty": int(item.get("quantity", 0))} for item in offers]

        ict_ts = exchange_ts.astimezone(timezone(timedelta(hours=7)))
        date_str = ict_ts.strftime("%Y-%m-%d")

        return {
            "symbol":        q.get("symbol", symbol),
            "market_id":     q.get("marketId", ""),
            "board_id":      q.get("boardId", "G1"),
            "bid_price1":    bp1,
            "bid_qty1":      bq1,
            "bid_price2":    bp2,
            "bid_qty2":      bq2,
            "bid_price3":    bp3,
            "bid_qty3":      bq3,
            "ask_price1":    ap1,
            "ask_qty1":      aq1,
            "ask_price2":    ap2,
            "ask_qty2":      aq2,
            "ask_price3":    ap3,
            "ask_qty3":      aq3,
            "total_bid_qty": int(q["totalBidQtty"]) if q.get("totalBidQtty") is not None else None,
            "total_ask_qty": int(q["totalOfferQtty"]) if q.get("totalOfferQtty") is not None else None,
            "bid_levels":    bid_levels,
            "ask_levels":    ask_levels,
            "exchange_ts":   _to_ms_precision(exchange_ts),
            "dnse_ts":       None,
            "producer_ts":   _to_ms_precision(datetime.now(timezone.utc)),
            "date":          date_str,
            "kafka_partition": None,
            "kafka_offset":    None,
        }
    except Exception as e:
        print(f"  [WARN] Skip invalid quote delta record: {e} | data={q!r:.120}")
        return None


def _parse_price_level(level_list: list, idx: int) -> tuple[float | None, int | None]:
    """Get price and quantity from level list at idx (0-indexed)."""
    if idx < len(level_list):
        lvl = level_list[idx]
        return lvl.get("price"), lvl.get("quantity")
    return None, None


def _quote_record_to_row(q: dict, symbol: str) -> tuple | None:
    """Map quote API record to DB insert tuple."""
    try:
        time_str = q.get("time", "")
        if not time_str:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                ict = timezone(timedelta(hours=7))
                naive = datetime.strptime(time_str, fmt)
                exchange_ts = naive.replace(tzinfo=ict).astimezone(timezone.utc)
                break
            except ValueError:
                pass
        else:
            return None

        bids   = q.get("bid", [])
        offers = q.get("offer", [])

        bp1, bq1 = _parse_price_level(bids, 0)
        bp2, bq2 = _parse_price_level(bids, 1)
        bp3, bq3 = _parse_price_level(bids, 2)
        ap1, aq1 = _parse_price_level(offers, 0)
        ap2, aq2 = _parse_price_level(offers, 1)
        ap3, aq3 = _parse_price_level(offers, 2)

        return (
            q.get("symbol", symbol),
            q.get("marketId", ""),
            bp1, bq1,
            bp2, bq2,
            bp3, bq3,
            ap1, aq1,
            ap2, aq2,
            ap3, aq3,
            exchange_ts,
            True,  # is_backfill
        )
    except Exception as e:
        print(f"  [WARN] Skip invalid quote record: {e} | data={q!r:.120}")
        return None


def _quote_fingerprint(row: tuple) -> tuple:
    """Get fingerprint from row: (symbol, exchange_ts_ms, bid_price1_rounded, ask_price1_rounded)."""
    # row[14] = exchange_ts, row[2]=bid_price1, row[8]=ask_price1
    exchange_ts_ms = int(row[14].timestamp() * 1000)
    bp1 = round(float(row[2]), 4) if row[2] is not None else None
    ap1 = round(float(row[8]), 4) if row[8] is not None else None
    return (row[0], exchange_ts_ms, bp1, ap1)


def backfill_quote(
    client: DNSEClient,
    conn,
    symbol: str,
    from_ts: datetime,
    to_ts: datetime,
    write_db: bool,
    write_minio: bool = False,
    overwrite: bool = False,
) -> dict:
    """
    Backfill quote data for a symbol.
    Returns stats: {fetched, skipped_dup, inserted, pages}.
    """
    stats = {"fetched": 0, "skipped_dup": 0, "inserted": 0, "pages": 0}
    storage_options = _build_storage_options()

    if write_minio:
        _ensure_bucket_exists()

    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        region_name="us-east-1",
    )
    ckpt_mgr = CheckpointManager(s3_client, bucket="market-data", data_type="quote")

    if overwrite:
        ckpt_mgr.clear_checkpoints_for_range(symbol, None, from_ts, to_ts)
        # DB Overwrite
        if write_db and conn is not None:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM order_book_l2 WHERE symbol = %s AND exchange_ts >= %s AND exchange_ts <= %s",
                        (symbol, from_ts, to_ts)
                    )
                conn.commit()
                print(f"  [OVERWRITE-DB] Deleted existing records in DB for range [{from_ts.isoformat()}, {to_ts.isoformat()}]")
            except Exception as e:
                print(f"  [WARN] Failed to delete existing DB records: {e}")

        # MinIO Overwrite
        if write_minio:
            try:
                dt = DeltaTable(QUOTE_DELTA_TABLE_URI, storage_options=storage_options)
                from_date = (from_ts + timedelta(hours=7)).date().isoformat()
                to_date = (to_ts + timedelta(hours=7)).date().isoformat()
                predicate = f"symbol = '{symbol}' AND date >= '{from_date}' AND date <= '{to_date}'"
                print(f"  [OVERWRITE-MINIO] Deleting existing data: {predicate}")
                dt.delete(predicate=predicate)
            except TableNotFoundError:
                pass
            except Exception as e:
                print(f"  [WARN] Failed to delete existing Delta Table records: {e}")

    if write_db and not _check_retention(from_ts, "order_book_l2", RETENTION_QUOTE_DAYS):
        cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_QUOTE_DAYS)
        print(
            f"  [WARN] from_ts={from_ts.isoformat()} exceeds retention limit of {RETENTION_QUOTE_DAYS} days "
            f"(cutoff={cutoff.date()}). Skipping database insertion."
        )
        write_db = False

    # Check checkpoints
    last_page, resume_token = 0, None
    if not overwrite:
        last_page, resume_token = ckpt_mgr.get_last_history_page(symbol, from_ts, to_ts)
        if last_page > 0:
            if not resume_token:
                print(f"  [CHECKPOINT] Range [{from_ts.isoformat()}, {to_ts.isoformat()}] already completed. Skipping.")
                return stats
            print(f"  [CHECKPOINT] Resuming from page {last_page} (nextPageToken: {resume_token})")
            stats["pages"] = last_page

    # Fetch existing DB fingerprints for deduplication
    existing_fps: set = set()
    if write_db and not overwrite and conn is not None:
        print("  [DEDUP-DB] Fetching existing fingerprints from DB...")
        existing_fps = _fetch_existing_quote_fps(conn, symbol, from_ts, to_ts)
        print(f"  [DEDUP-DB] Found {len(existing_fps)} records in range [from, to]")

    # Fetch existing Delta fingerprints for deduplication
    existing_fps_delta: set = set()
    if write_minio and not overwrite:
        print("  [DEDUP-MINIO] Fetching existing fingerprints from Delta Lake...")
        existing_fps_delta = _fetch_existing_quote_fps_delta(symbol, from_ts, to_ts, storage_options)
        print(f"  [DEDUP-MINIO] Found {len(existing_fps_delta)} records in range [from, to]")

    from_epoch = _to_epoch(from_ts)
    to_epoch   = _to_epoch(to_ts)

    next_token = resume_token
    page_num = last_page + 1
    while True:
        status, body = _api_request_with_retry(
            client.get_quotes,
            symbol=symbol,
            board_id="G1",
            from_date=from_epoch,
            to_date=to_epoch,
            limit=API_LIMIT,
            order="ASC",
            next_page_token=next_token,
        )

        if status != 200:
            print(f"  [ERROR] API returned HTTP {status}. Terminating pagination.")
            break

        data = _parse_body(body)
        quotes = data.get("quotes", [])
        stats["pages"] = page_num
        stats["fetched"] += len(quotes)
        print(f"  [PAGE {page_num}] Received {len(quotes)} quotes")

        db_rows_written = 0
        minio_rows_written = 0

        if write_db and quotes and conn is not None:
            rows_to_insert = []
            for q in quotes:
                row = _quote_record_to_row(q, symbol)
                if row is None:
                    continue
                fp = _quote_fingerprint(row)
                if not overwrite and fp in existing_fps:
                    stats["skipped_dup"] += 1
                else:
                    rows_to_insert.append(row)
                    existing_fps.add(fp)

            if rows_to_insert:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur, _QUOTE_INSERT_SQL, rows_to_insert,
                        template=None, page_size=len(rows_to_insert),
                    )
                conn.commit()
                stats["inserted"] += len(rows_to_insert)
                db_rows_written = len(rows_to_insert)
                print(f"  [INSERT-DB] +{len(rows_to_insert)} rows | skip_dup={stats['skipped_dup']}")

        if write_minio and quotes:
            delta_rows = []
            skipped_dup_delta = 0
            for q in quotes:
                row = _quote_record_to_row_delta(q, symbol)
                if row is None:
                    continue
                exchange_ts_ms = int(row["exchange_ts"].timestamp() * 1000)
                bp1 = round(float(row["bid_price1"]), 4) if row["bid_price1"] is not None else None
                ap1 = round(float(row["ask_price1"]), 4) if row["ask_price1"] is not None else None
                fp = (row["symbol"], exchange_ts_ms, bp1, ap1)

                if not overwrite and fp in existing_fps_delta:
                    skipped_dup_delta += 1
                else:
                    delta_rows.append(row)
                    existing_fps_delta.add(fp)

            if delta_rows:
                df = pd.DataFrame(delta_rows)
                table = pa.Table.from_pandas(df, schema=QUOTE_ARROW_SCHEMA)
                write_deltalake(
                    QUOTE_DELTA_TABLE_URI,
                    table,
                    mode="append",
                    partition_by=["date"],
                    storage_options=storage_options,
                    schema_mode="merge",
                )
                minio_rows_written = len(delta_rows)
                print(f"  [INSERT-MINIO] +{len(delta_rows)} rows to Delta Lake | skipped_dup={skipped_dup_delta}")
            else:
                print("  [CHUNK] No new rows to write to Delta Lake (all are duplicates).")

        next_token_next = data.get("nextPageToken") or None
        
        # Write checkpoint for this page
        ckpt_key = ckpt_mgr.get_history_key(symbol, from_ts, to_ts, page_num)
        ckpt_mgr.write_checkpoint(ckpt_key, {
            "symbol": symbol,
            "chunk_start": from_ts.isoformat(),
            "chunk_end": to_ts.isoformat(),
            "page_num": page_num,
            "records_written": max(db_rows_written, minio_rows_written),
            "next_page_token": next_token_next,
        })

        next_token = next_token_next
        page_num += 1
        
        if not next_token or not quotes:
            break
        time.sleep(API_PAGE_DELAY)

    return stats


def _ensure_instrument_registered(conn, symbol: str, to_ts: datetime):
    """
    Ensure physical derivative symbol is registered in instrument_master so ohlcv_live works.
    If not present, infer final_trade_date from data maximum timestamp and insert skeleton record.
    """
    if conn is None:
        return
    # Only applies to derivative codes with format 41I1... or 41I2... and length = 9
    if not (symbol.startswith("41I1") or symbol.startswith("41I2")) or len(symbol) != 9:
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM instrument_master WHERE symbol = %s;", (symbol,))
            if cur.fetchone():
                return  # already exists

            # Default to to_ts if database is empty, then query actual max timestamp
            final_date = to_ts.date()
            cur.execute("SELECT MAX(exchange_ts) FROM market_trade WHERE symbol = %s;", (symbol,))
            r = cur.fetchone()
            if r and r[0]:
                final_date = r[0].date()
            else:
                cur.execute("SELECT MAX(exchange_ts) FROM order_book_l2 WHERE symbol = %s;", (symbol,))
                r = cur.fetchone()
                if r and r[0]:
                    final_date = r[0].date()

            symbol_type = "VN30F1M" if symbol.startswith("41I1") else "V100F1M"

            cur.execute(
                """
                INSERT INTO instrument_master (
                    symbol, market_id, security_group_id, symbol_type, final_trade_date, is_active
                ) VALUES (%s, 'DVX', 'FU', %s, %s, false)
                ON CONFLICT (symbol, market_id) DO NOTHING;
                """,
                (symbol, symbol_type, final_date)
            )
            conn.commit()
            print(f"  [METADATA] Registered skeleton record for {symbol} ({symbol_type}, final_trade_date={final_date}) in instrument_master.")
    except Exception as e:
        print(f"[WARN] Failed to auto register instrument {symbol}: {e}")


def _resolve_physical_symbol_for_day(contracts: list[dict], symbol_type: str, d: date) -> str | None:
    """
    contracts: list of dicts {"symbol": str, "final_trade_date": date} sorted by final_trade_date asc.
    symbol_type: e.g. "VN30F1M", "VN30F2M", "VN30F1Q", "VN30F2Q"
    d: date to resolve for
    """
    active = [c for c in contracts if c["final_trade_date"] >= d]
    if not active:
        return None

    contract_class = symbol_type[-3:]

    if contract_class == "F1M":
        return active[0]["symbol"]
    elif contract_class == "F2M":
        if len(active) > 1:
            return active[1]["symbol"]
        return None

    if len(active) < 2:
        return None

    f2m_date = active[1]["final_trade_date"]
    quarters = [c for c in active if c["final_trade_date"].month in (3, 6, 9, 12) and c["final_trade_date"] > f2m_date]

    if contract_class == "F1Q":
        if len(quarters) > 0:
            return quarters[0]["symbol"]
    elif contract_class == "F2Q":
        if len(quarters) > 1:
            return quarters[1]["symbol"]

    return None


def _resolve_derivative_symbols(conn, symbol: str, from_ts: datetime, to_ts: datetime) -> list[dict]:
    """
    Check and map rolling derivative symbol (VN30F1M, VN30F2M, ...)
    to their physical contract symbols for each sub-interval in range [from_ts, to_ts].
    """
    if conn is None:
        return []

    prefix = None
    if symbol.startswith("VN30"):
        prefix = "41I1"
    elif symbol.startswith("V100"):
        prefix = "41I2"

    if prefix is None:
        return []

    if not (symbol.endswith("F1M") or symbol.endswith("F2M") or symbol.endswith("F1Q") or symbol.endswith("F2Q")):
        return []

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, final_trade_date
                FROM instrument_master
                WHERE symbol LIKE %s AND market_id = 'DVX' AND security_group_id = 'FU' AND final_trade_date IS NOT NULL
                ORDER BY final_trade_date ASC;
                """,
                (prefix + '%',)
            )
            rows = cur.fetchall()
    except Exception as e:
        print(f"[WARN] Failed to query instrument_master to resolve symbol: {e}")
        return []

    if not rows:
        return []

    contracts = [{"symbol": r[0], "final_trade_date": r[1]} for r in rows]

    from_ict = from_ts + timedelta(hours=7)
    to_ict = to_ts + timedelta(hours=7)

    current_date = from_ict.date()
    end_date = to_ict.date()

    day_mappings = {}
    d = current_date
    while d <= end_date:
        phys = _resolve_physical_symbol_for_day(contracts, symbol, d)
        if phys:
            day_mappings[d] = phys
        d += timedelta(days=1)

    if not day_mappings:
        return []

    intervals = []
    current_phys = None
    start_dt = None

    d = current_date
    while d <= end_date:
        phys = day_mappings.get(d)

        if phys != current_phys:
            if current_phys is not None:
                end_dt = datetime.combine(d, datetime.min.time()) - timedelta(microseconds=1)
                intervals.append({
                    "phys_symbol": current_phys,
                    "from_ts": start_dt,
                    "to_ts": end_dt
                })
            current_phys = phys
            start_dt = datetime.combine(d, datetime.min.time())

        d += timedelta(days=1)

    if current_phys is not None:
        intervals.append({
            "phys_symbol": current_phys,
            "from_ts": start_dt,
            "to_ts": datetime.combine(end_date + timedelta(days=1), datetime.min.time()) - timedelta(microseconds=1)
        })

    resolved = []
    for interval in intervals:
        utc_from = interval["from_ts"] - timedelta(hours=7)
        utc_from = utc_from.replace(tzinfo=timezone.utc)
        utc_to = interval["to_ts"] - timedelta(hours=7)
        utc_to = utc_to.replace(tzinfo=timezone.utc)

        overlap_from = max(from_ts, utc_from)
        overlap_to = min(to_ts, utc_to)
        if overlap_from < overlap_to:
            resolved.append({
                "phys_symbol": interval["phys_symbol"],
                "from_ts": overlap_from,
                "to_ts": overlap_to
            })

    return resolved


# ── Public API ────────────────────────────────────────────────────
def run_backfill(
    symbol: str,
    data_type: str,
    from_ts_str: str,
    to_ts_str: str,
    target: str = "db",
    overwrite: bool = False,
) -> dict:
    """
    Main runner function that can be imported or executed directly.
    """
    if not DNSE_API_KEY or not DNSE_API_SECRET:
        raise ValueError("DNSE_API_KEY and DNSE_API_SECRET must be configured in .env")

    from_ts = _parse_ts(from_ts_str)
    to_ts   = _parse_ts(to_ts_str)

    if from_ts >= to_ts:
        raise ValueError(f"from_ts ({from_ts_str}) must be less than to_ts ({to_ts_str})")

    write_db    = target in ("db", "both")
    write_minio = target in ("minio", "both")

    client = DNSEClient(
        api_key=DNSE_API_KEY,
        api_secret=DNSE_API_SECRET,
        base_url=DNSE_BASE_URL,
    )

    conn = None
    try:
        conn = _create_db_conn()
    except Exception as e:
        print(f"[WARN] Cannot connect to DB to resolve symbols or write data: {e}")

    results = {}
    if data_type in ("trade", "all"):
        results["trade"] = {"fetched": 0, "skipped_dup": 0, "inserted": 0, "pages": 0}
    if data_type in ("quote", "all"):
        results["quote"] = {"fetched": 0, "skipped_dup": 0, "inserted": 0, "pages": 0}

    # Expand index constituents if symbol is an index
    sym_upper = symbol.upper().strip()
    target_symbols = [sym_upper]
    if sym_upper in ("VN30", "VN100", "HNX30"):
        print(f"\n[INDEX GROUP] Resolving constituent symbols for index: {sym_upper}")
        if conn is not None:
            try:
                with conn.cursor() as cur:
                    index_pattern = f"(^|,){sym_upper}(,|$)"
                    cur.execute(
                        "SELECT DISTINCT symbol FROM instrument_master WHERE is_active = true AND index_name ~ %s ORDER BY symbol",
                        (index_pattern,)
                    )
                    constituents = [r[0] for r in cur.fetchall()]
                if constituents:
                    print(f"  [INDEX GROUP] Resolved {len(constituents)} constituent symbols.")
                    target_symbols = constituents
            except Exception as e:
                print(f"  [WARN] Failed to resolve index constituents: {e}. Using '{sym_upper}' as index itself.")

    symbols_to_verify = []

    try:
        for tsym in target_symbols:
            resolved_contracts = _resolve_derivative_symbols(conn, tsym, from_ts, to_ts)

            if resolved_contracts:
                print(f"\n[RESOLVE] Symbol '{tsym}' resolved to contracts:")
                for rc in resolved_contracts:
                    print(f"  - {rc['phys_symbol']}: {rc['from_ts'].isoformat()} -> {rc['to_ts'].isoformat()} (UTC)")
                    symbols_to_verify.append(rc["phys_symbol"])

                for rc in resolved_contracts:
                    phys_symbol = rc["phys_symbol"]
                    sub_from = rc["from_ts"]
                    sub_to = rc["to_ts"]

                    if data_type in ("trade", "all"):
                        print(f"\n--- Backfill TRADE for {phys_symbol} ({tsym}) [{sub_from.isoformat()} -> {sub_to.isoformat()}] ---")
                        res = backfill_trade(client, conn, phys_symbol, sub_from, sub_to, write_db, write_minio, overwrite)
                        for k in results["trade"]:
                            results["trade"][k] += res[k]
                        print(
                            f"--- TRADE for {phys_symbol} completed: fetch={res['fetched']} pages={res['pages']} "
                            f"inserted={res['inserted']} skip_dup={res['skipped_dup']} ---\n"
                        )

                    if data_type in ("quote", "all"):
                        print(f"\n--- Backfill QUOTE for {phys_symbol} ({tsym}) [{sub_from.isoformat()} -> {sub_to.isoformat()}] ---")
                        res = backfill_quote(client, conn, phys_symbol, sub_from, sub_to, write_db, write_minio, overwrite)
                        for k in results["quote"]:
                            results["quote"][k] += res[k]
                        print(
                            f"--- QUOTE for {phys_symbol} completed: fetch={res['fetched']} pages={res['pages']} "
                            f"inserted={res['inserted']} skip_dup={res['skipped_dup']} ---\n"
                        )
            else:
                symbols_to_verify.append(tsym)
                print(f"\n{'='*60}")
                print(f"[BACKFILL] Symbol={tsym} | Type={data_type}")
                print(f"[BACKFILL] From={from_ts.isoformat()} -> To={to_ts.isoformat()} (UTC)")
                print(f"[BACKFILL] Target=DB:{write_db}, MinIO:{write_minio}")
                print(f"{'='*60}\n")

                if data_type in ("trade", "all"):
                    print(f"--- Starting backfill TRADE for {tsym} ---")
                    res = backfill_trade(client, conn, tsym, from_ts, to_ts, write_db, write_minio, overwrite)
                    for k in results["trade"]:
                        results["trade"][k] += res[k]
                    print(
                        f"--- TRADE complete: fetch={res['fetched']} pages={res['pages']} "
                        f"inserted={res['inserted']} skip_dup={res['skipped_dup']} ---\n"
                    )

                if data_type in ("quote", "all"):
                    print(f"--- Starting backfill QUOTE for {tsym} ---")
                    res = backfill_quote(client, conn, tsym, from_ts, to_ts, write_db, write_minio, overwrite)
                    for k in results["quote"]:
                        results["quote"][k] += res[k]
                    print(
                        f"--- QUOTE complete: fetch={res['fetched']} pages={res['pages']} "
                        f"inserted={res['inserted']} skip_dup={res['skipped_dup']} ---\n"
                    )

                _ensure_instrument_registered(conn, tsym, to_ts)

    finally:
        if conn is not None:
            conn.close()

    # ── Post-Backfill Verification ───────────────────────────────────
    print("\n=== POST-BACKFILL VERIFICATION ===")
    storage_options = _build_storage_options()
        
    for tsym in symbols_to_verify:
        if data_type in ("trade", "all"):
            print(f"\nVerification for TRADE - {tsym}:")
            try:
                dt = DeltaTable(TRADE_DELTA_TABLE_URI, storage_options=storage_options)
                tbl = dt.to_pyarrow_table(
                    columns=["exchange_ts"],
                    partitions=[("symbol", "=", tsym)]
                )
                if len(tbl) > 0:
                    df_ts = tbl.column("exchange_ts").to_pandas()
                    min_t = df_ts.min().replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=7)))
                    max_t = df_ts.max().replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=7)))
                    print(f"  [Delta Lake] Rows: {len(tbl)} | Range (ICT): {min_t.strftime('%Y-%m-%d %H:%M:%S')} -> {max_t.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    print("  [Delta Lake] 0 rows found.")
            except TableNotFoundError:
                print("  [Delta Lake] Table not found.")
            except Exception as e:
                print(f"  [Delta Lake] Error: {e}")
                
            try:
                verify_conn = _create_db_conn()
                with verify_conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*), MIN(exchange_ts), MAX(exchange_ts) FROM market_trade WHERE symbol = %s",
                        (tsym,)
                    )
                    row = cur.fetchone()
                    if row and row[0] > 0 and row[1] and row[2]:
                        cnt, db_min, db_max = row
                        db_min_ict = db_min.astimezone(timezone(timedelta(hours=7)))
                        db_max_ict = db_max.astimezone(timezone(timedelta(hours=7)))
                        print(f"  [TimescaleDB] Rows: {cnt} | Range (ICT): {db_min_ict.strftime('%Y-%m-%d %H:%M:%S')} -> {db_max_ict.strftime('%Y-%m-%d %H:%M:%S')}")
                    else:
                        print("  [TimescaleDB] 0 rows found.")
                verify_conn.close()
            except Exception as e:
                print(f"  [TimescaleDB] Error: {e}")

        if data_type in ("quote", "all"):
            print(f"\nVerification for QUOTE - {tsym}:")
            try:
                dt = DeltaTable(QUOTE_DELTA_TABLE_URI, storage_options=storage_options)
                tbl = dt.to_pyarrow_table(
                    columns=["exchange_ts"],
                    partitions=[("symbol", "=", tsym)]
                )
                if len(tbl) > 0:
                    df_ts = tbl.column("exchange_ts").to_pandas()
                    min_t = df_ts.min().replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=7)))
                    max_t = df_ts.max().replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=7)))
                    print(f"  [Delta Lake] Rows: {len(tbl)} | Range (ICT): {min_t.strftime('%Y-%m-%d %H:%M:%S')} -> {max_t.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    print("  [Delta Lake] 0 rows found.")
            except TableNotFoundError:
                print("  [Delta Lake] Table not found.")
            except Exception as e:
                print(f"  [Delta Lake] Error: {e}")
                
            try:
                verify_conn = _create_db_conn()
                with verify_conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*), MIN(exchange_ts), MAX(exchange_ts) FROM order_book_l2 WHERE symbol = %s",
                        (tsym,)
                    )
                    row = cur.fetchone()
                    if row and row[0] > 0 and row[1] and row[2]:
                        cnt, db_min, db_max = row
                        db_min_ict = db_min.astimezone(timezone(timedelta(hours=7)))
                        db_max_ict = db_max.astimezone(timezone(timedelta(hours=7)))
                        print(f"  [TimescaleDB] Rows: {cnt} | Range (ICT): {db_min_ict.strftime('%Y-%m-%d %H:%M:%S')} -> {db_max_ict.strftime('%Y-%m-%d %H:%M:%S')}")
                    else:
                        print("  [TimescaleDB] 0 rows found.")
                verify_conn.close()
            except Exception as e:
                print(f"  [TimescaleDB] Error: {e}")

    print(f"\n{'='*60}")
    print("[DONE] Backfill Summary:")
    for dtype, s in results.items():
        print(
            f"  {dtype.upper()}: fetched={s['fetched']} | pages={s['pages']} | "
            f"inserted={s['inserted']} | skip_dup={s['skipped_dup']}"
        )
    print(f"{'='*60}\n")

    return results


# ── CLI entry point ───────────────────────────────────────────────
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill historical market data from DNSE REST API into TimescaleDB / Delta Lake",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python ingestion/handlers/backfill_trade_quote.py \\
    --symbol VCB --type trade \\
    --from "2026-06-23 10:00:00" --to "2026-06-23 10:15:00" \\
    --target db

  python ingestion/handlers/backfill_trade_quote.py \\
    --symbol VN30F1M --type all \\
    --from "2026-06-23 09:15:00" --to "2026-06-23 15:00:00" \\
    --target both --overwrite

Note:
  - Input timestamps are assumed to be in ICT (UTC+7), e.g. "2026-06-23 10:00:00"
  - Retention limit: trade<=30 days, quote<=7 days (must satisfy window to write to DB)
  - Limit per page: 1000 records (DNSE API limit)
""",
    )
    parser.add_argument("--symbol",  required=True,
                        help="Ticker symbol, e.g. VCB, FPT, VN30F1M")
    parser.add_argument("--type",    required=True,
                        choices=["trade", "quote", "all"],
                        help="Data type to backfill")
    parser.add_argument("--from",    required=True, dest="from_ts",
                        help="Start time ICT, e.g. '2026-06-23 10:00:00'")
    parser.add_argument("--to",      required=True, dest="to_ts",
                        help="End time ICT, e.g. '2026-06-23 10:15:00'")
    parser.add_argument("--target",  default="db",
                        choices=["db", "minio", "both"],
                        help="Target storage option (default: db)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing data range instead of skipping duplicates")
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    run_backfill(
        symbol=args.symbol,
        data_type=args.type,
        from_ts_str=args.from_ts,
        to_ts_str=args.to_ts,
        target=args.target,
        overwrite=args.overwrite,
    )
