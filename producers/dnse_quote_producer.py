import asyncio
import os
import signal
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)
from dotenv import load_dotenv

# --- local imports ---
from trading_websocket import TradingClient
from trading_websocket.models import Quote

load_dotenv()

# -- Config ----------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("kafka_bootstrap_servers", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("schema_registry_url", "http://localhost:8081")
KAFKA_TOPIC = "market.orderbook-l2"

DNSE_API_KEY = os.getenv("DNSE_API_KEY")
DNSE_API_SECRET = os.getenv("DNSE_API_SECRET")
DNSE_WS_URL = "wss://ws-openapi.dnse.com.vn"

SYMBOLS = ["ACB","FPT", "VIC", "SSI", "HPG", "MWG"]

# -- Schema ----------------------------------------------------------------
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "order_book_l2.avsc"


def _load_avro_schema() -> str:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return f.read()


def _safe_price(levels, idx):
    """Lay price tu PriceLevel list an toan."""
    if levels and len(levels) > idx and levels[idx].price is not None:
        return float(levels[idx].price)
    return None


def _safe_qty(levels, idx):
    """Lay quantity tu PriceLevel list an toan."""
    if levels and len(levels) > idx and levels[idx].quantity is not None:
        return int(levels[idx].quantity)
    return None


def _quote_to_dict(quote: Quote, _ctx) -> dict:
    """Convert Quote -> dict theo Avro schema."""
    event_ms = int(quote.sendingTime * 1000) if quote.sendingTime else None
    received_ms = int(quote.multicastReceiveTime * 1000) if quote.multicastReceiveTime else None

    # Fallback neu sendingTime is None
    if event_ms is None:
        event_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    return {
        "symbol": quote.symbol,
        "market_id": quote.marketId if quote.marketId else "UNKNOWN",
        "board_id": quote.boardId if quote.boardId else "UNKNOWN",
        "bid_price1": _safe_price(quote.bid, 0),
        "bid_qty1": _safe_qty(quote.bid, 0),
        "bid_price2": _safe_price(quote.bid, 1),
        "bid_qty2": _safe_qty(quote.bid, 1),
        "bid_price3": _safe_price(quote.bid, 2),
        "bid_qty3": _safe_qty(quote.bid, 2),
        "ask_price1": _safe_price(quote.offer, 0),
        "ask_qty1": _safe_qty(quote.offer, 0),
        "ask_price2": _safe_price(quote.offer, 1),
        "ask_qty2": _safe_qty(quote.offer, 1),
        "ask_price3": _safe_price(quote.offer, 2),
        "ask_qty3": _safe_qty(quote.offer, 2),
        "total_bid_qty": int(quote.totalBidQtty) if quote.totalBidQtty is not None else None,
        "total_ask_qty": int(quote.totalOfferQtty) if quote.totalOfferQtty is not None else None,
        "event_ts": event_ms,
        "received_ts": received_ms,
    }


# -- Kafka setup -----------------------------------------------------------
def _create_producer():
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry,
        schema_str=_load_avro_schema(),
        to_dict=_quote_to_dict,
    )

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 50,
        "batch.num.messages": 500,
        "compression.type": "lz4",
        "acks": "1",
    })

    key_serializer = StringSerializer("utf_8")

    return producer, avro_serializer, key_serializer


def _delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err} | topic={msg.topic()} key={msg.key()}")


# -- Main ------------------------------------------------------------------
async def main():
    producer, avro_serializer, key_serializer = _create_producer()
    msg_count = 0

    def handle_quote(quote: Quote):
        nonlocal msg_count

        try:
            key = key_serializer(quote.symbol)
            value = avro_serializer(
                quote,
                SerializationContext(KAFKA_TOPIC, MessageField.VALUE),
            )

            producer.produce(
                topic=KAFKA_TOPIC,
                key=key,
                value=value,
                on_delivery=_delivery_report,
            )
            msg_count += 1

        except BufferError:
            producer.poll(0)
            try:
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=key,
                    value=value,
                    on_delivery=_delivery_report,
                )
                msg_count += 1
            except BufferError:
                print(f"[WARN] Buffer full after poll, dropping: {quote.symbol}")

        except Exception as e:
            print(f"[ERROR] Produce failed: {e}")

    # -- DNSE WebSocket --
    encoding = "json"
    client = TradingClient(
        api_key=DNSE_API_KEY,
        api_secret=DNSE_API_SECRET,
        base_url=DNSE_WS_URL,
        encoding=encoding,
    )

    print(f"[START] DNSE Quote WebSocket -> Kafka ({KAFKA_BOOTSTRAP})")
    print(f"[CONFIG] Topic: {KAFKA_TOPIC} | Symbols: {SYMBOLS}")

    await client.connect()
    print("[SUCCESS] Connected to DNSE WebSocket!")

    await client.subscribe_quotes(
        symbols=SYMBOLS,
        on_quote=handle_quote,
        encoding=encoding,
    )
    print(f"[SUBSCRIBED] Listening quotes for {len(SYMBOLS)} symbols...")

    # -- Graceful shutdown --
    shutdown = asyncio.Event()

    def _signal_handler():
        print("\n[STOP] Shutting down...")
        shutdown.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    try:
        while not shutdown.is_set():
            producer.poll(0)
            await asyncio.sleep(0.1)
    except KeyboardInterrupt:
        print("\n[STOP] Shutting down...")
    finally:
        remaining = producer.flush(timeout=10)
        print(f"[DONE] Total: {msg_count} quotes | Remaining in buffer: {remaining}")


if __name__ == "__main__":
    asyncio.run(main())
