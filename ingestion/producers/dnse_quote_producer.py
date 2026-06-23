"""
producers/dnse_quote_producer.py

Publishes DNSE L2 order book (Quote) events to Kafka topic: market.orderbook-l2
"""
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingestion import DnseKafkaProducer

from dnse.websocket.models import Quote

from ingestion.common.symbol_resolver import SymbolResolver

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_TOPIC = "market.orderbook-l2"

_resolver = SymbolResolver()
print(f"[CONFIG] Filter: {_resolver.describe()}")

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "order_book_l2.avsc"


# ── Helpers ───────────────────────────────────────────────────────
def _safe_price(levels, idx):
    if levels and len(levels) > idx and levels[idx].price is not None:
        return float(levels[idx].price)
    return None


def _safe_qty(levels, idx):
    if levels and len(levels) > idx and levels[idx].quantity is not None:
        return int(levels[idx].quantity)
    return None


def _levels_to_list(levels) -> list:
    if not levels:
        return []
    return [
        {
            "price": float(lvl.price)   if lvl.price    is not None else None,
            "qtty":  int(lvl.quantity)  if lvl.quantity is not None else None,
        }
        for lvl in levels
    ]


# ── to_dict ───────────────────────────────────────────────────────
def _quote_to_dict(quote: Quote, _ctx) -> dict:
    from datetime import datetime, timezone
    exchange_ms = int(quote.time * 1000)                 if quote.time                 else None
    dnse_ms     = int(quote.multicastReceiveTime * 1000) if quote.multicastReceiveTime else None
    producer_ms = int(quote.receivedAt * 1000)           if quote.receivedAt           else None

    if exchange_ms is None:
        exchange_ms = dnse_ms or int(datetime.now(timezone.utc).timestamp() * 1000)

    return {
        "symbol":       quote.symbol,
        "market_id":    quote.marketId if quote.marketId else "UNKNOWN",
        "board_id":     quote.boardId  if quote.boardId  else "UNKNOWN",
        "bid_price1":   _safe_price(quote.bid, 0),
        "bid_qty1":     _safe_qty(quote.bid, 0),
        "bid_price2":   _safe_price(quote.bid, 1),
        "bid_qty2":     _safe_qty(quote.bid, 1),
        "bid_price3":   _safe_price(quote.bid, 2),
        "bid_qty3":     _safe_qty(quote.bid, 2),
        "ask_price1":   _safe_price(quote.offer, 0),
        "ask_qty1":     _safe_qty(quote.offer, 0),
        "ask_price2":   _safe_price(quote.offer, 1),
        "ask_qty2":     _safe_qty(quote.offer, 1),
        "ask_price3":   _safe_price(quote.offer, 2),
        "ask_qty3":     _safe_qty(quote.offer, 2),
        "total_bid_qty":int(quote.totalBidQtty)   if quote.totalBidQtty   is not None else None,
        "total_ask_qty":int(quote.totalOfferQtty) if quote.totalOfferQtty is not None else None,
        "bid_levels":   _levels_to_list(quote.bid),
        "ask_levels":   _levels_to_list(quote.offer),
        "exchange_ts":  exchange_ms,
        "dnse_ts":      dnse_ms,
        "producer_ts":  producer_ms,
    }


# ── Main ──────────────────────────────────────────────────────────
async def main():
    symbols = _resolver.resolve()
    print(f"[CONFIG] Symbols ({len(symbols)}): {symbols}")

    async def _resubscribe(client, new_symbols: list[str]):
        """Hot-reload: subscribe to additional symbols only."""
        await client.subscribe_quotes(
            symbols=new_symbols,
            on_quote=lambda quote: producer.produce(quote.symbol, quote),
            encoding="msgpack",
        )
        print(f"[SUBSCRIBED] +{len(new_symbols)} symbols via hot-reload")

    producer = DnseKafkaProducer(
        topic=KAFKA_TOPIC,
        schema_path=SCHEMA_PATH,
        to_dict_fn=_quote_to_dict,
        producer_config={"linger.ms": 50, "batch.num.messages": 500},
        service_name="p-quote",
        symbol_resolver=_resolver,
        resubscribe_fn=_resubscribe,
    )

    async def subscribe_fn(client):
        await client.subscribe_quotes(
            symbols=symbols,
            on_quote=lambda quote: producer.produce(quote.symbol, quote),
            encoding="msgpack",
        )
        print(f"[SUBSCRIBED] quotes for {len(symbols)} symbols")

    await producer.run(subscribe_fn)


if __name__ == "__main__":
    asyncio.run(main())
