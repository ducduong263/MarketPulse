"""
export_secdef.py — REST API fallback sync for Security Definition.

Goal:
  - Find symbols missing security_definition rows for today
    (UPCOM/HCX and any symbols missed due to WSS timeout)
  - Call DNSE REST API get_security_definition for each symbol
  - Insert/Upsert into security_definition table

Rate limit: 1000 requests/hour => sleep 3.8s between requests
"""

import json
import os
import sys
import time
from datetime import datetime, date, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# -- SDK path setup -------------------------------------------------------
# Works in both local dev and Docker/Airflow (mounted at /opt/airflow/sdk)
_SDK_CANDIDATES = [
    Path(__file__).resolve().parents[3] / "sdk" / "openapi-sdk" / "python",
    Path("/opt/airflow/sdk/openapi-sdk/python"),
]
for _sdk_path in _SDK_CANDIDATES:
    if _sdk_path.exists() and str(_sdk_path) not in sys.path:
        sys.path.insert(0, str(_sdk_path))
        break

from dnse import DNSEClient

load_dotenv()

# -- Config ---------------------------------------------------------------
DB_HOST = os.getenv("postgres_host", "localhost")
DB_PORT = os.getenv("postgres_port", "5432")
DB_NAME = os.getenv("postgres_db", "market_data")
DB_USER = os.getenv("postgres_user", "marketpulse")
DB_PASSWORD = os.getenv("postgres_password", "mp_secret_2026")

DNSE_API_KEY = os.getenv("DNSE_API_KEY")
DNSE_API_SECRET = os.getenv("DNSE_API_SECRET")
DNSE_BASE_URL = "https://openapi.dnse.com.vn"

# Rate limit: 1000 req/hour => max ~1 req every 3.6s
# Use 3.8s to keep a safe buffer
RATE_LIMIT_SLEEP = 3.8

INSERT_SQL = """
INSERT INTO security_definition (
    symbol, market_id, board_id, isin, product_grp_id, security_group_id,
    basic_price, ceiling_price, floor_price, open_interest_qty,
    security_status, admin_status, trading_method_status, trading_sanction_status,
    listing_date, final_trade_date, trading_date
) VALUES %s
ON CONFLICT (symbol, market_id, board_id, trading_date)
DO UPDATE SET
    basic_price = EXCLUDED.basic_price,
    ceiling_price = EXCLUDED.ceiling_price,
    floor_price = EXCLUDED.floor_price,
    security_status = EXCLUDED.security_status,
    admin_status = EXCLUDED.admin_status,
    trading_method_status = EXCLUDED.trading_method_status,
    trading_sanction_status = EXCLUDED.trading_sanction_status,
    ingested_ts = now();
"""


def _create_db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def _get_missing_symbols(conn) -> list[str]:
    """
    Get the list of symbols missing today's security_definition.
    Conditions:
      - Present in instrument_master with final_trade_date >= today OR NULL
      - NOT already present in security_definition with trading_date = today
    Sorted by market_id DESC (UPX first, HCX after) to prioritize.
    """
    sql = """
        SELECT DISTINCT symbol
        FROM instrument_master
        WHERE (final_trade_date >= CURRENT_DATE OR final_trade_date IS NULL)
          AND symbol NOT IN (
              SELECT DISTINCT symbol
              FROM security_definition
              WHERE trading_date = CURRENT_DATE
          )
        ORDER BY symbol;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    symbols = [row[0] for row in rows]
    print(f"[DB] Found {len(symbols)} symbols missing from today's security_definition")
    return symbols




MAX_RETRY = 3
RETRY_DELAY = 2


def _parse_date(date_str) -> date | None:
    """Convert string ISO or YYYYMMDD -> date object."""
    if not date_str:
        return None
    try:
        if isinstance(date_str, date):
            return date_str
        s = str(date_str)
        if "-" in s:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        return datetime.strptime(s[:8], "%Y%m%d").date()
    except Exception:
        return None


def _fetch_with_retry(client, symbol: str):
    status, body = 500, None
    for attempt in range(1, MAX_RETRY + 1):
        status, body = client.get_security_definition(symbol=symbol, board_id="G1", dry_run=False)
        if status == 200:
            return status, body
        print(f"[WARN] {symbol}: HTTP {status} (attempt {attempt}/{MAX_RETRY}) - {body}")
        
        if status == 404:
            break
            
        if attempt < MAX_RETRY:
            time.sleep(RETRY_DELAY * attempt)
    return status, body


def main():
    print("[START] Export Security Definition to DB (REST API Fallback)")

    conn = _create_db_conn()
    client = DNSEClient(
        api_key=DNSE_API_KEY,
        api_secret=DNSE_API_SECRET,
        base_url=DNSE_BASE_URL,
    )

    # Find missing symbols
    symbols = _get_missing_symbols(conn)
    if not symbols:
        print("[INFO] No missing symbols found -- security_definition is fully synced for today.")
        conn.close()
        return

    today_date = datetime.now(timezone.utc).date()
    success_count = 0
    fail_count = 0

    for idx, sym in enumerate(symbols, 1):
        print(f"[{idx}/{len(symbols)}] Fetching: {sym}")

        status, body = _fetch_with_retry(client, sym)
        if status != 200:
            print(f"[WARN] API Error for {sym}: HTTP {status} - {body}")
            fail_count += 1
        else:
            if isinstance(body, str):
                try:
                    body = json.loads(body)
                except json.JSONDecodeError:
                    print(f"[ERROR] Failed to parse JSON for {sym}")
                    fail_count += 1
                    # Sleep before moving to the next symbol
                    time.sleep(RATE_LIMIT_SLEEP)
                    continue

            records = body if isinstance(body, list) else [body]
            rows_to_insert = []

            for record in records:
                if not isinstance(record, dict) or "symbol" not in record:
                    continue

                # Only save board G1 (primary board) for optimization
                if record.get("boardId") != "G1":
                    continue

                row = (
                    record.get("symbol", ""),
                    record.get("marketId", ""),
                    record.get("boardId", ""),
                    record.get("isin"),
                    record.get("productGrpId"),
                    record.get("securityGroupId"),
                    record.get("basicPrice"),
                    record.get("ceilingPrice"),
                    record.get("floorPrice"),
                    record.get("openInterestQuantity"),
                    record.get("securityStatus"),
                    record.get("symbolAdminStatusCode"),
                    record.get("symbolTradingMethodStatusCode"),
                    record.get("symbolTradingSanctionStatusCode"),
                    _parse_date(record.get("ListingDate") or record.get("listingDate")),
                    _parse_date(record.get("finalTradeDate")),
                    today_date,
                )
                rows_to_insert.append(row)

            if rows_to_insert:
                try:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_values(
                            cur, INSERT_SQL, rows_to_insert,
                            template=None,
                            page_size=len(rows_to_insert),
                        )
                    conn.commit()
                    success_count += 1
                except Exception as e:
                    conn.rollback()
                    print(f"[ERROR] DB insert failed for {sym}: {e}")
                    fail_count += 1
            else:
                print(f"[WARN] No valid records returned for {sym}")
                fail_count += 1

        # Rate limiting -- space out requests to stay below 1000 req/hour limit
        if idx < len(symbols):
            time.sleep(RATE_LIMIT_SLEEP)

    print(
        f"\n[DONE] Total: {len(symbols)} symbols | "
        f"Success: {success_count} | Failed: {fail_count}"
    )
    conn.close()


if __name__ == "__main__":
    main()
