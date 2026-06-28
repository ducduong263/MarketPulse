import asyncio
import argparse
import os
import signal
import sys
import time
from datetime import date
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ── SDK path setup ────────────────────────────────────────────────
# Works in both local dev and Docker/Airflow (mounted at /opt/airflow/sdk)
_SDK_CANDIDATES = [
    Path(__file__).resolve().parents[3] / "sdk" / "openapi-sdk" / "python",
    Path("/opt/airflow/sdk/openapi-sdk/python"),
]
for _sdk_path in _SDK_CANDIDATES:
    if _sdk_path.exists() and str(_sdk_path) not in sys.path:
        sys.path.insert(0, str(_sdk_path))
        break

from dnse import TradingClient
from dnse.websocket.models import SecurityDefinition

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
DNSE_API_KEY    = os.environ.get("DNSE_API_KEY")
DNSE_API_SECRET = os.environ.get("DNSE_API_SECRET")

if not DNSE_API_KEY or not DNSE_API_SECRET:
    raise ValueError("DNSE_API_KEY and DNSE_API_SECRET must be set in environment")
DNSE_WS_URL     = "wss://ws-openapi.dnse.com.vn"

DB_HOST     = os.getenv("postgres_host", "localhost")
DB_PORT     = os.getenv("postgres_port", "5432")
DB_NAME     = os.getenv("postgres_db", "market_data")
DB_USER     = os.getenv("postgres_user", "marketpulse")
DB_PASSWORD = os.getenv("postgres_password", "mp_secret_2026")

FLUSH_INTERVAL = 10.0

# Priority group to subscribe via WSS (HOSE + HNX + Derivatives)
# UPCOM (UPX) and HCX will be fetched via REST API in export_secdef.py
WSS_PRIORITY_MARKETS = ("STO", "STX", "DVX")
WSS_MAX_SYMBOLS = 1950  # Hard limit: 2000 channels; keep 50 buffer to prevent errors

# ── SQL ───────────────────────────────────────────────────────────
UPSERT_SQL = """
INSERT INTO security_definition (
    symbol, market_id, board_id, isin, product_grp_id, security_group_id,
    basic_price, ceiling_price, floor_price, open_interest_qty,
    security_status, admin_status, trading_method_status, trading_sanction_status,
    listing_date, final_trade_date
) VALUES %s
ON CONFLICT (symbol, market_id, board_id, trading_date)
DO UPDATE SET
    isin                    = EXCLUDED.isin,
    product_grp_id          = EXCLUDED.product_grp_id,
    security_group_id       = EXCLUDED.security_group_id,
    basic_price             = EXCLUDED.basic_price,
    ceiling_price           = EXCLUDED.ceiling_price,
    floor_price             = EXCLUDED.floor_price,
    open_interest_qty       = EXCLUDED.open_interest_qty,
    security_status         = EXCLUDED.security_status,
    admin_status            = EXCLUDED.admin_status,
    trading_method_status   = EXCLUDED.trading_method_status,
    trading_sanction_status = EXCLUDED.trading_sanction_status,
    listing_date            = EXCLUDED.listing_date,
    final_trade_date        = EXCLUDED.final_trade_date,
    ingested_ts             = clock_timestamp()
"""


# ── Helpers ───────────────────────────────────────────────────────
def _to_float(v) -> float | None:
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def _to_int(v) -> int | None:
    try:
        return int(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def _to_date(v) -> date | None:
    """parse_timestamp(date_only=True) returns 'YYYY-MM-DD' string or None."""
    if v is None:
        return None
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v)[:10])
    except (ValueError, TypeError):
        return None


def _sd_to_row(sd: SecurityDefinition) -> tuple:
    return (
        sd.symbol,
        sd.marketId,
        sd.boardId,
        sd.isin,
        sd.productGrpId,
        sd.securityGroupId,
        _to_float(sd.basicPrice),
        _to_float(sd.ceilingPrice),
        _to_float(sd.floorPrice),
        _to_int(sd.openInterestQuantity),
        sd.securityStatus,
        sd.symbolAdminStatusCode,
        sd.symbolTradingMethodStatusCode,
        sd.symbolTradingSanctionStatusCode,
        _to_date(sd.listingDate),
        _to_date(sd.finalTradeDate),
    )


# ── DB ────────────────────────────────────────────────────────────
def _create_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    )


def _get_priority_symbols(conn) -> list[str]:
    """
    Get the list of priority symbols from instrument_master:
    - STO (HOSE), STX (HNX), DVX (Derivatives) markets
    - Not expired yet: final_trade_date >= today OR NULL
    - Limited by WSS_MAX_SYMBOLS to prevent MAX_CHANNELS_EXCEEDED errors
    """
    markets_ph = ",".join(["%s"] * len(WSS_PRIORITY_MARKETS))
    sql = f"""
        SELECT DISTINCT symbol
        FROM instrument_master
        WHERE market_id IN ({markets_ph})
          AND (final_trade_date >= CURRENT_DATE OR final_trade_date IS NULL)
        ORDER BY symbol
        LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, list(WSS_PRIORITY_MARKETS) + [WSS_MAX_SYMBOLS])
        rows = cur.fetchall()
    symbols = [row[0] for row in rows]
    print(f"[DB] Loaded {len(symbols)} priority symbols from instrument_master "
          f"(markets: {', '.join(WSS_PRIORITY_MARKETS)})")
    return symbols


def _flush(conn, pending: dict, stats: dict) -> None:
    """Upsert all pending records into DB."""
    if not pending:
        return
    rows = list(pending.values())
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=len(rows))
        conn.commit()
        stats["upserted"] += len(rows)
        print(f"[FLUSH] +{len(rows)} rows upserted | unique symbols today: {stats['upserted']}")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Flush failed: {e}")


# ── Main ──────────────────────────────────────────────────────────
async def main(timeout: int | None):
    conn = _create_db_conn()

    pending: dict[tuple, tuple] = {}
    stats = {"received": 0, "upserted": 0}
    last_flush = time.monotonic()

    shutdown = asyncio.Event()

    def _signal_handler():
        print("\n[STOP] Shutting down...")
        shutdown.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass  # Windows fallback

    def handle_secdef(sd: SecurityDefinition):
        if not sd.symbol or not sd.marketId or not sd.boardId:
            return
        key = (sd.symbol, sd.marketId, sd.boardId)
        pending[key] = _sd_to_row(sd)
        stats["received"] += 1
        if stats["received"] % 100 == 0:
            print(f"[INFO] Received: {stats['received']} | Pending: {len(pending)}")

    client = TradingClient(
        api_key=DNSE_API_KEY,
        api_secret=DNSE_API_SECRET,
        base_url=DNSE_WS_URL,
        encoding="msgpack",
    )

    print(f"[START] SecDef Sync -> PostgreSQL ({DB_HOST}:{DB_PORT}/{DB_NAME})")
    print(f"[CONFIG] Timeout: {'indefinite' if timeout is None else f'{timeout}s'} | Flush every: {FLUSH_INTERVAL}s")

    # Load the priority symbols from the DB
    priority_symbols = _get_priority_symbols(conn)
    if not priority_symbols:
        print("[ERROR] No priority symbols found in instrument_master. "
              "Run dag_instrument_delta (first run) before this script.")
        conn.close()
        return

    await client.connect()
    print("[SUCCESS] Connected to DNSE WebSocket!")

    # Subscribe in batches to prevent MAX_CHANNELS_EXCEEDED server error
    BATCH_SIZE = 300
    for i in range(0, len(priority_symbols), BATCH_SIZE):
        batch = priority_symbols[i : i + BATCH_SIZE]
        await client.subscribe_sec_def(
            symbols=batch,
            on_sec_def=handle_secdef,
            encoding="msgpack",
            board_id="G1",
        )
        print(f"[SUBSCRIBED] Batch {i // BATCH_SIZE + 1}: {len(batch)} symbols "
              f"(total so far: {min(i + BATCH_SIZE, len(priority_symbols))}/{len(priority_symbols)})")
    print(f"[SUBSCRIBED] Done subscribing {len(priority_symbols)} priority symbols. Listening...")

    deadline = (time.monotonic() + timeout) if timeout else None

    try:
        while not shutdown.is_set():
            await asyncio.sleep(0.5)

            now = time.monotonic()

            # Flush periodically
            if pending and (now - last_flush) >= FLUSH_INTERVAL:
                _flush(conn, pending, stats)
                pending.clear()
                last_flush = now

            # Check timeout
            if deadline and now >= deadline:
                print(f"[TIMEOUT] {timeout}s elapsed, stopping.")
                break

    finally:
        # Final flush
        if pending:
            print(f"[FINAL FLUSH] {len(pending)} remaining records...")
            _flush(conn, pending, stats)

        await client.disconnect()
        conn.close()
        print(f"\n[DONE] Total received: {stats['received']} | Upserted to DB: {stats['upserted']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync SecurityDefinition via DNSE WebSocket")
    parser.add_argument(
        "--timeout", type=int, default=None,
        help="Stop after N seconds (default: run until Ctrl+C)",
    )
    args = parser.parse_args()
    asyncio.run(main(args.timeout))
