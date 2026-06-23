"""
producers/dnse_expected_price_producer.py

Publishes DNSE expected price events to Kafka topic: market.expected-price
"""
import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingestion import DnseKafkaProducer

from dnse.websocket.models import ExpectedPrice

from ingestion.common.symbol_resolver import SymbolResolver

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_TOPIC = "market.expected-price"
BOARD_ID    = "G1"

_resolver = SymbolResolver()
print(f"[CONFIG] Filter: {_resolver.describe()}")
SYMBOLS = _resolver.resolve()

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "expected_price.avsc"


# ── Helpers ───────────────────────────────────────────────────────
def _to_int(v) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


# ── to_dict ───────────────────────────────────────────────────────
def _ep_to_dict(ep: ExpectedPrice, _ctx) -> dict:
    received_ms = (
        int(ep.receivedAt * 1000) if ep.receivedAt
        else int(datetime.now(timezone.utc).timestamp() * 1000)
    )
    return {
        "symbol":         ep.symbol,
        "market_id":      ep.marketId,
        "board_id":       ep.boardId,
        "isin":           ep.isin,
        "close_price":    ep.closePrice,
        "expected_price": ep.expectedTradePrice,
        "expected_qty":   _to_int(ep.expectedTradeQuantity),
        "producer_ts":    received_ms,
    }


# ── Main ──────────────────────────────────────────────────────────
async def main():
    producer = DnseKafkaProducer(
        topic=KAFKA_TOPIC,
        schema_path=SCHEMA_PATH,
        to_dict_fn=_ep_to_dict,
        producer_config={"linger.ms": 100},
        service_name="p-expected-price",
    )

    print(f"[CONFIG] Board: {BOARD_ID} | Symbols: {SYMBOLS}")

    async def subscribe_fn(client):
        await client.subscribe_expected_price(
            SYMBOLS,
            on_expected_price=lambda ep: producer.produce(ep.symbol, ep),
            encoding="msgpack",
            board_id=BOARD_ID,
        )
        print(f"[SUBSCRIBED] expected_price for {len(SYMBOLS)} symbols")

    await producer.run(subscribe_fn)


if __name__ == "__main__":
    asyncio.run(main())
