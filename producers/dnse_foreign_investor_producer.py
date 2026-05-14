import asyncio
import os
import signal
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from dotenv import load_dotenv

from dnse import TradingClient
from dnse.websocket.models import ForeignInvestor

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP     = os.getenv("kafka_bootstrap_servers", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("schema_registry_url", "http://localhost:8081")
KAFKA_TOPIC         = "market.foreign-investor"

DNSE_API_KEY    = os.getenv("DNSE_API_KEY")
DNSE_API_SECRET = os.getenv("DNSE_API_SECRET")
DNSE_WS_URL     = "wss://ws-openapi.dnse.com.vn"

# Subscribe tat ca board (*) de nhan du lieu NĐT nuoc ngoai tren toan thi truong
SYMBOLS = [
    "VIC", "VHM", "VNM", "GAS", "SAB", "MSN", "CTG", "BID", "VCB", "TCB",
    "MBB", "ACB", "STB", "HDB", "VPB", "HPG", "SSI", "FPT", "MWG", "VRE",
    "PLX", "POW", "PVD", "BSR", "DGC", "DPM", "DCM", "GVR", "HVN", "SHB",
]
BOARD_ID = "*"  # Tat ca board

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "foreign_investor.avsc"


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
        "trading_session_id": str(fi.tradingSessionId) if fi.tradingSessionId is not None else None,

        "sell_volume": _to_int(fi.sellVolume),
        "sell_value":  _to_int(fi.sellTradedAmount),
        "buy_volume":  _to_int(fi.buyVolume),
        "buy_value":   _to_int(fi.buyTradedAmount),

        "total_sell_volume": _to_int(fi.totalSellVolume),
        "total_sell_value":  _to_int(fi.totalSellTradedAmount),
        "total_buy_volume":  _to_int(fi.totalBuyVolume),
        "total_buy_value":   _to_int(fi.totalBuyTradedAmount),

        "order_limit_qty":  _to_int(fi.foreignerOrderLimitQuantity),
        "buy_possible_qty": _to_int(fi.foreignerBuyPossibleQuantity),

        "exchange_ts": exchange_ms,
        "producer_ts": producer_ms,
    }


# ── Kafka setup ───────────────────────────────────────────────────
def _create_producer():
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(
        schema_registry_client=sr,
        schema_str=_load_avro_schema(),
        to_dict=_fi_to_dict,
    )
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms":         200,
        "compression.type":  "lz4",
        "acks":              "1",
    })
    return producer, avro_serializer, StringSerializer("utf_8")


def _delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err} | topic={msg.topic()}")


# ── Main ──────────────────────────────────────────────────────────
async def main():
    producer, avro_serializer, key_serializer = _create_producer()
    msg_count = 0

    def handle_foreign_investor(fi: ForeignInvestor):
        nonlocal msg_count
        try:
            key   = key_serializer(fi.symbol)
            value = avro_serializer(fi, SerializationContext(KAFKA_TOPIC, MessageField.VALUE))
            producer.produce(
                topic=KAFKA_TOPIC, key=key, value=value,
                on_delivery=_delivery_report,
            )
            msg_count += 1
            if msg_count % 50 == 0:
                print(f"[INFO] msg_count={msg_count}")
            producer.poll(0)
        except BufferError:
            producer.poll(0.1)
            try:
                producer.produce(
                    topic=KAFKA_TOPIC, key=key, value=value,
                    on_delivery=_delivery_report,
                )
                msg_count += 1
            except BufferError:
                print(f"[WARN] Buffer full, dropping: {fi.symbol}")
        except Exception as e:
            print(f"[ERROR] Produce failed for {fi.symbol}: {e}")

    encoding = "msgpack"
    client = TradingClient(
        api_key=DNSE_API_KEY,
        api_secret=DNSE_API_SECRET,
        base_url=DNSE_WS_URL,
        encoding=encoding,
    )

    print(f"[START] Foreign Investor Producer -> Kafka ({KAFKA_BOOTSTRAP})")
    print(f"[CONFIG] Topic: {KAFKA_TOPIC} | Board: {BOARD_ID} | Symbols: {len(SYMBOLS)}")

    await client.connect()
    print("[SUCCESS] Connected to DNSE WebSocket!")

    await client.subscribe_foreign_trading(
        symbols=SYMBOLS,
        board_id=BOARD_ID,
        on_trade=handle_foreign_investor,
        encoding=encoding,
    )
    print(f"[SUBSCRIBED] Listening to {len(SYMBOLS)} symbols on board {BOARD_ID}...")

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
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
