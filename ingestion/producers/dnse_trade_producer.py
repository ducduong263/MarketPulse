"""
producers/dnse_trade_producer.py

Publishes DNSE trade_extra events to Kafka topic: market.trade
"""
import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingestion import DnseKafkaProducer

from dnse.websocket.models import TradeExtra

from ingestion.common.symbol_resolver import SymbolResolver

load_dotenv()

# ── Config ─────────────────────────────────────────────────────
KAFKA_TOPIC = "market.trade"

_resolver = SymbolResolver()
print(f"[CONFIG] Filter: {_resolver.describe()}")

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "market_trade.avsc"


# ── to_dict ───────────────────────────────────────────────────────
def _trade_to_dict(trade: TradeExtra, _ctx) -> dict:
    exchange_ms = int(trade.time * 1000)                 if trade.time                 else None
    dnse_ms     = int(trade.multicastReceiveTime * 1000) if trade.multicastReceiveTime else None
    producer_ms = int(trade.receivedAt * 1000)           if trade.receivedAt           else None

    if exchange_ms is None:
        exchange_ms = dnse_ms or int(datetime.now(timezone.utc).timestamp() * 1000)

    side_val = 0
    if isinstance(trade.side, int):
        side_val = trade.side
    elif isinstance(trade.side, str):
        _s = trade.side.strip().upper()
        if _s in ("BUY", "B", "1"):
            side_val = 1
        elif _s in ("SELL", "S", "2"):
            side_val = 2

    return {
        "symbol":      trade.symbol,
        "market_id":   trade.marketId  if trade.marketId  else "UNKNOWN",
        "board_id":    trade.boardId   if trade.boardId   else "UNKNOWN",
        "price":       trade.price,
        "quantity":    trade.quantity                  if trade.quantity            is not None else 0,
        "side":        side_val,
        "session_vol": trade.totalVolumeTraded         if trade.totalVolumeTraded   is not None else None,
        "session_high":trade.highestPrice              if trade.highestPrice        is not None else None,
        "session_low": trade.lowestPrice               if trade.lowestPrice         is not None else None,
        "session_open":trade.openPrice                 if trade.openPrice           is not None else None,
        "session_vwap":trade.avgPrice                  if trade.avgPrice            is not None else None,
        "trading_session_id": trade.tradingSessionId,
        "exchange_ts": exchange_ms,
        "dnse_ts":     dnse_ms,
        "producer_ts": producer_ms,
    }


# ── Main ─────────────────────────────────────────────────────
async def main():
    symbols = _resolver.resolve()
    print(f"[CONFIG] Symbols ({len(symbols)}): {symbols}")

    async def _resubscribe(client, new_symbols: list[str]):
        """Hot-reload: subscribe to additional symbols only."""
        await client.subscribe_trade_extra(
            symbols=new_symbols,
            on_trade_extra=lambda trade: producer.produce(trade.symbol, trade),
            encoding="msgpack",
        )
        print(f"[SUBSCRIBED] +{len(new_symbols)} symbols via hot-reload")

    producer = DnseKafkaProducer(
        topic=KAFKA_TOPIC,
        schema_path=SCHEMA_PATH,
        to_dict_fn=_trade_to_dict,
        producer_config={"linger.ms": 50, "batch.num.messages": 500},
        service_name="p-trade",
        symbol_resolver=_resolver,
        resubscribe_fn=_resubscribe,
    )

    async def subscribe_fn(client):
        await client.subscribe_trade_extra(
            symbols=symbols,
            on_trade_extra=lambda trade: producer.produce(trade.symbol, trade),
            encoding="msgpack",
        )
        print(f"[SUBSCRIBED] trade_extra for {len(symbols)} symbols")

    await producer.run(subscribe_fn)


if __name__ == "__main__":
    asyncio.run(main())