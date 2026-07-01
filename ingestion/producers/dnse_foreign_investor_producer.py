"""
producers/dnse_foreign_investor_producer.py

Publishes DNSE foreign investor trading events to Kafka topic: market.foreign-investor
"""
import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingestion import DnseKafkaProducer

from dnse.websocket.models import ForeignInvestor

from ingestion.common.symbol_resolver import SymbolResolver

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_TOPIC = "market.foreign-investor"
BOARD_ID    = "G1"

_resolver = SymbolResolver()
print(f"[CONFIG] Filter: {_resolver.describe()}")
SYMBOLS = _resolver.resolve()

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "foreign_investor.avsc"


# ── Helpers ───────────────────────────────────────────────────────
def _to_int(v) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


# ── to_dict ───────────────────────────────────────────────────────
def _fi_to_dict(fi: ForeignInvestor, _ctx) -> dict:
    exchange_ms = int(fi.transactTime * 1000) if fi.transactTime else None
    producer_ms = (
        int(fi.receivedAt * 1000) if fi.receivedAt
        else int(datetime.now(timezone.utc).timestamp() * 1000)
    )
    return {
        "symbol":             fi.symbol,
        "market_id":          fi.marketId,
        "board_id":           fi.boardId,
        "trading_session_id": fi.tradingSessionId,
        "sell_volume":        _to_int(fi.sellVolume),
        "sell_value":         _to_int(fi.sellTradedAmount),
        "buy_volume":         _to_int(fi.buyVolume),
        "buy_value":          _to_int(fi.buyTradedAmount),
        "total_sell_volume":  _to_int(fi.totalSellVolume),
        "total_sell_value":   _to_int(fi.totalSellTradedAmount),
        "total_buy_volume":   _to_int(fi.totalBuyVolume),
        "total_buy_value":    _to_int(fi.totalBuyTradedAmount),
        "order_limit_qty":    _to_int(fi.foreignerOrderLimitQuantity),
        "buy_possible_qty":   _to_int(fi.foreignerBuyPossibleQuantity),
        "exchange_ts":        exchange_ms,
        "producer_ts":        producer_ms,
    }


# ── Main ──────────────────────────────────────────────────────────
async def main():
    producer = DnseKafkaProducer(
        topic=KAFKA_TOPIC,
        schema_path=SCHEMA_PATH,
        to_dict_fn=_fi_to_dict,
        producer_config={"linger.ms": 200},
        service_name="p-foreign-investor",
    )

    print(f"[CONFIG] Board: {BOARD_ID} | Symbols: {len(SYMBOLS)}")

    async def subscribe_fn(client):
        await client.subscribe_foreign_trading(
            symbols=SYMBOLS,
            board_id=BOARD_ID,
            on_trade=lambda fi: producer.produce(fi.symbol, fi),
            encoding="msgpack",
        )
        print(f"[SUBSCRIBED] foreign_trading for {len(SYMBOLS)} symbols on board {BOARD_ID}")

    await producer.run(subscribe_fn)


if __name__ == "__main__":
    asyncio.run(main())
