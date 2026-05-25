import asyncio
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from dotenv import load_dotenv

from dnse import TradingClient
from dnse.websocket.models import ExpectedPrice

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP     = os.getenv("kafka_bootstrap_servers", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("schema_registry_url", "http://localhost:8081")
KAFKA_TOPIC         = "market.expected-price"

DNSE_API_KEY    = os.getenv("DNSE_API_KEY")
DNSE_API_SECRET = os.getenv("DNSE_API_SECRET")
DNSE_WS_URL     = "wss://ws-openapi.dnse.com.vn"

SYMBOLS = [
    "VIC", "VHM", "VNM", "GAS", "SAB", "MSN", "CTG", "BID", "VCB", "TCB",
    "MBB", "ACB", "STB", "HDB", "VPB", "HPG", "SSI", "FPT", "MWG", "VRE",
]
BOARD_ID = "G1"

SCHEMA_PATH = Path(__file__).resolve().parent.parent.parent / "schemas" / "expected_price.avsc"

# ── Helpers ───────────────────────────────────────────────────────
def _load_avro_schema() -> str:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return f.read()


def _to_int(v) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _ep_to_dict(ep: ExpectedPrice, _ctx) -> dict:
    received_ms = int(ep.receivedAt * 1000) if ep.receivedAt else int(datetime.now(timezone.utc).timestamp() * 1000)
    return {
        "symbol":         ep.symbol,
        "market_id":      ep.marketId,
        "board_id":       ep.boardId,
        "isin":           ep.isin,
        "close_price":    float(ep.closePrice)           if ep.closePrice           is not None else None,
        "expected_price": float(ep.expectedTradePrice)   if ep.expectedTradePrice   is not None else None,
        "expected_qty":   _to_int(ep.expectedTradeQuantity),
        "producer_ts":    received_ms,
    }


# ── Kafka setup ───────────────────────────────────────────────────
def _create_producer():
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(
        schema_registry_client=sr,
        schema_str=_load_avro_schema(),
        to_dict=_ep_to_dict,
    )
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 100,
        "compression.type": "lz4",
        "acks": "1",
    })
    return producer, avro_serializer, StringSerializer("utf_8")


def _delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err} | topic={msg.topic()}")


# ── Main ──────────────────────────────────────────────────────────
async def main():
    producer, avro_serializer, key_serializer = _create_producer()
    msg_count = 0

    def handle_expected_price(ep: ExpectedPrice):
        nonlocal msg_count
        try:
            key   = key_serializer(ep.symbol)
            value = avro_serializer(ep, SerializationContext(KAFKA_TOPIC, MessageField.VALUE))
            producer.produce(topic=KAFKA_TOPIC, key=key, value=value, on_delivery=_delivery_report)
            msg_count += 1
        except BufferError:
            producer.poll(0)
            try:
                producer.produce(topic=KAFKA_TOPIC, key=key, value=value, on_delivery=_delivery_report)
                msg_count += 1
            except BufferError:
                print(f"[WARN] Buffer full, dropping: {ep.symbol}")
        except Exception as e:
            print(f"[ERROR] Produce failed for {ep.symbol}: {e}")

    encoding = "msgpack"
    client = TradingClient(
        api_key=DNSE_API_KEY,
        api_secret=DNSE_API_SECRET,
        base_url=DNSE_WS_URL,
        encoding=encoding,
    )

    print(f"[START] Expected Price Producer -> Kafka ({KAFKA_BOOTSTRAP})")
    print(f"[CONFIG] Topic: {KAFKA_TOPIC} | Board: {BOARD_ID} | Symbols: {SYMBOLS}")

    await client.connect()
    print("[SUCCESS] Connected to DNSE WebSocket!")

    await client.subscribe_expected_price(
        SYMBOLS,
        on_expected_price=handle_expected_price,
        encoding=encoding,
        board_id=BOARD_ID,
    )
    print(f"[SUBSCRIBED] Listening to {len(SYMBOLS)} symbols...")

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
        print(f"[DONE] Total: {msg_count} messages | Remaining: {remaining}")


if __name__ == "__main__":
    asyncio.run(main())
