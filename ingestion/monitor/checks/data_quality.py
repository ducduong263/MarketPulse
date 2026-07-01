"""
Data quality check — queries TimescaleDB for price and spread anomalies
ingested in the last 70 seconds.

1. Price Anomaly: Trades with price > ceiling_price or price < floor_price.
2. Spread Anomaly: Quotes with spread < 0 (ask_price1 < bid_price1).
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime

import psycopg2

logger = logging.getLogger(__name__)

_DB_CONFIG = {
    "host":     os.getenv("postgres_host", "timescaledb"),
    "port":     int(os.getenv("postgres_port", "5432")),
    "dbname":   os.getenv("postgres_db", "market_data"),
    "user":     os.getenv("postgres_user", "marketpulse"),
    "password": os.getenv("postgres_password", ""),
}


@dataclass
class PriceAnomaly:
    symbol: str
    price: float
    ceiling: float
    floor: float
    exchange_ts: datetime


@dataclass
class SpreadAnomaly:
    symbol: str
    bid: float
    ask: float
    spread: float
    exchange_ts: datetime


def check_price_anomalies(interval_seconds: int = 70) -> list[PriceAnomaly]:
    """Find trades with price exceeding limits ingested recently."""
    anomalies: list[PriceAnomaly] = []
    conn = psycopg2.connect(**_DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  t.symbol,
                  t.price,
                  sd.ceiling_price,
                  sd.floor_price,
                  t.exchange_ts
                FROM market_trade t
                JOIN security_definition sd
                  ON t.symbol = sd.symbol
                 AND t.market_id = sd.market_id
                 AND sd.trading_date = (NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh')::date
                WHERE t.ingested_ts > NOW() - (%s * INTERVAL '1 second')
                  AND sd.ceiling_price IS NOT NULL
                  AND (t.price > sd.ceiling_price OR t.price < sd.floor_price)
                ORDER BY t.exchange_ts DESC
                """,
                (interval_seconds,)
            )
            for row in cur.fetchall():
                anomalies.append(PriceAnomaly(
                    symbol=row[0],
                    price=row[1],
                    ceiling=row[2],
                    floor=row[3],
                    exchange_ts=row[4],
                ))
    finally:
        conn.close()
    return anomalies


def check_spread_anomalies(interval_seconds: int = 70) -> list[SpreadAnomaly]:
    """Find quotes with spread < 0 (ask < bid) ingested recently."""
    anomalies: list[SpreadAnomaly] = []
    conn = psycopg2.connect(**_DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  symbol,
                  bid_price1,
                  ask_price1,
                  spread,
                  exchange_ts
                FROM order_book_l2
                WHERE ingested_ts > NOW() - (%s * INTERVAL '1 second')
                  AND bid_price1 IS NOT NULL
                  AND ask_price1 IS NOT NULL
                  AND bid_price1 > 0
                  AND spread < 0
                ORDER BY exchange_ts DESC
                """,
                (interval_seconds,)
            )
            for row in cur.fetchall():
                anomalies.append(SpreadAnomaly(
                    symbol=row[0],
                    bid=row[1],
                    ask=row[2],
                    spread=row[3],
                    exchange_ts=row[4],
                ))
    finally:
        conn.close()
    return anomalies
