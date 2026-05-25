import json
import os
import sys
import time
from datetime import datetime, date, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from dnse import DNSEClient

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
DB_HOST = os.getenv("postgres_host", "localhost")
DB_PORT = os.getenv("postgres_port", "5432")
DB_NAME = os.getenv("postgres_db", "market_data")
DB_USER = os.getenv("postgres_user", "marketpulse")
DB_PASSWORD = os.getenv("postgres_password", "mp_secret_2026")

DNSE_API_KEY = os.getenv("DNSE_API_KEY")
DNSE_API_SECRET = os.getenv("DNSE_API_SECRET")
DNSE_BASE_URL = "https://openapi.dnse.com.vn"

SYMBOLS = ["ACB", "FPT", "VIC", "SSI", "HPG", "MWG"]

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

MAX_RETRY = 3
RETRY_DELAY = 2

def _parse_date(date_str) -> date | None:
    """Convert string ISO hoặc YYYYMMDD -> date object."""
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
    for attempt in range(1, MAX_RETRY + 1):
        status, body = client.get_security_definition(symbol=symbol, board_id=None, dry_run=False)
        if status == 200:
            return status, body
        print(f"[WARN] {symbol}: HTTP {status} (attempt {attempt}/{MAX_RETRY}) - {body}")
        if attempt < MAX_RETRY:
            time.sleep(RETRY_DELAY * attempt)
    return status, body

def main():
    print(f"[START] Export Security Definition to DB")
    
    conn = _create_db_conn()

    client = DNSEClient(
        api_key=DNSE_API_KEY,
        api_secret=DNSE_API_SECRET,
        base_url=DNSE_BASE_URL,
    )

    today_date = datetime.now(timezone.utc).date()
    records_to_insert = []

    for sym in SYMBOLS:
        print(f"Fetching Security Definition for {sym}...")
        status, body = _fetch_with_retry(client, sym)
        if status != 200:
            print(f"[WARN] API Error for {sym}: HTTP {status} - {body}")
            continue

        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError:
                print(f"[ERROR] Failed to parse JSON for {sym}")
                continue

        records = body if isinstance(body, list) else [body]
        
        for record in records:
            if not isinstance(record, dict) or "symbol" not in record:
                continue

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
                today_date
            )
            records_to_insert.append(row)

    if records_to_insert:
        print(f"[INFO] Inserting {len(records_to_insert)} records to DB...")
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur, INSERT_SQL, records_to_insert,
                    template=None,
                    page_size=100
                )
            conn.commit()
            print("[SUCCESS] Data exported successfully!")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] DB Insert failed: {e}")
    else:
        print("[INFO] No data to insert.")

    conn.close()

if __name__ == "__main__":
    main()
