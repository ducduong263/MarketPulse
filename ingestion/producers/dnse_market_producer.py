import asyncio
import os
import signal
import sys
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

# --- SDK path (submodule at sdk/openapi-sdk) ---
_SDK = Path(__file__).resolve().parent.parent.parent / "sdk" / "openapi-sdk" / "python"
sys.path.insert(0, str(_SDK))
sys.path.insert(0, str(_SDK / "websocket-marketdata"))

from dnse import TradingClient
from dnse.websocket.models import TradeExtra

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("kafka_bootstrap_servers", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("schema_registry_url", "http://localhost:8081")
KAFKA_TOPIC = "market.trade"

DNSE_API_KEY = os.getenv("DNSE_API_KEY")
DNSE_API_SECRET = os.getenv("DNSE_API_SECRET")
DNSE_WS_URL = "wss://ws-openapi.dnse.com.vn"

SYMBOLS = ["ACB", "FPT", "VIC", "SSI", "HPG", "MWG", "41I1G5000"]

# ── Schema ────────────────────────────────────────────────────────
SCHEMA_PATH = Path(__file__).resolve().parent.parent.parent / "schemas" / "market_trade.avsc"


def _load_avro_schema() -> str:
    """Load Avro schema string from .avsc file."""
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return f.read()


def _trade_to_dict(trade: TradeExtra, _ctx) -> dict:
    """Convert TradeExtra -> dict theo Avro schema (dung lam to_dict cho AvroSerializer)."""
    # epoch float -> milliseconds int for Avro timestamp-millis
    exchange_ms = int(trade.time * 1000)                 if trade.time                 else None
    dnse_ms     = int(trade.multicastReceiveTime * 1000) if trade.multicastReceiveTime else None
    producer_ms = int(trade.receivedAt * 1000)           if trade.receivedAt           else None

    # Fallback: neu sendingTime la None, dung dnse_ms (cung NTP clock, dam bao exchange_ts <= dnse_ts)
    if exchange_ms is None:
        exchange_ms = dnse_ms or int(datetime.now(timezone.utc).timestamp() * 1000)

    # Parse side to int (1=buy, 2=sell) accounting for 'BUY'/'SELL' strings
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
        "symbol": trade.symbol,
        "market_id": trade.marketId if trade.marketId else "UNKNOWN",
        "board_id": trade.boardId if trade.boardId else "UNKNOWN",
        "price": float(trade.price),
        "quantity": int(trade.quantity) if trade.quantity is not None else 0,
        "side": side_val,
        "session_vol": int(trade.totalVolumeTraded) if trade.totalVolumeTraded is not None else None,
        "session_high": float(trade.highestPrice) if trade.highestPrice is not None else None,
        "session_low": float(trade.lowestPrice) if trade.lowestPrice is not None else None,
        "session_open": float(trade.openPrice) if trade.openPrice is not None else None,
        "session_vwap": float(trade.avgPrice) if trade.avgPrice is not None else None,
        "exchange_ts": exchange_ms,
        "dnse_ts":     dnse_ms,
        "producer_ts": producer_ms,
    }


# ── Kafka setup ──────────────────────────────────────────────────
def _create_producer():
    """Tạo Kafka Producer + AvroSerializer qua Schema Registry."""
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry,
        schema_str=_load_avro_schema(),
        to_dict=_trade_to_dict,
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
    """Callback khi message được delivered hoặc lỗi."""
    if err is not None:
        print(f"[ERROR] Delivery failed: {err} | topic={msg.topic()} key={msg.key()}")
    # Uncomment để debug:
    # else:
    #     print(f"[OK] {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


# ── Main ─────────────────────────────────────────────────────────
async def main():
    producer, avro_serializer, key_serializer = _create_producer()
    msg_count = 0

    def handle_trade(trade: TradeExtra):
        nonlocal msg_count

        try:
            key = key_serializer(trade.symbol)
            value = avro_serializer(
                trade,
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
                print(f"[WARN] Buffer full after poll, dropping: {trade.symbol}@{trade.price}")

        except Exception as e:
            print(f"[ERROR] Produce failed: {e}")

    # ── DNSE WebSocket ──
    encoding = "json"
    client = TradingClient(
        api_key=DNSE_API_KEY,
        api_secret=DNSE_API_SECRET,
        base_url=DNSE_WS_URL,
        encoding=encoding,
    )

    print(f"[START] DNSE WebSocket -> Kafka ({KAFKA_BOOTSTRAP})")
    print(f"[CONFIG] Topic: {KAFKA_TOPIC} | Symbols: {SYMBOLS}")

    await client.connect()
    print("[SUCCESS] Connected to DNSE WebSocket!")

    await client.subscribe_trade_extra(
        symbols=SYMBOLS,
        on_trade_extra=handle_trade,
        encoding=encoding,
    )
    print(f"[SUBSCRIBED] Listening trade_extra for {len(SYMBOLS)} symbols...")

    # ── Graceful shutdown ──
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
        print(f"[DONE] Total: {msg_count} trades | Remaining in buffer: {remaining}")


if __name__ == "__main__":
    asyncio.run(main())