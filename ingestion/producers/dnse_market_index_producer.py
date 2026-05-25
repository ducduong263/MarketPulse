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

_SDK = Path(__file__).resolve().parent.parent.parent / "sdk" / "openapi-sdk" / "python"
sys.path.insert(0, str(_SDK))
sys.path.insert(0, str(_SDK / "websocket-marketdata"))

from dnse import TradingClient
from dnse.websocket.models import MarketIndex

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP     = os.getenv("kafka_bootstrap_servers", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("schema_registry_url", "http://localhost:8081")
KAFKA_TOPIC         = "market.index"

DNSE_API_KEY    = os.getenv("DNSE_API_KEY")
DNSE_API_SECRET = os.getenv("DNSE_API_SECRET")
DNSE_WS_URL     = "wss://ws-openapi.dnse.com.vn"

MARKET_INDICES = ["VNINDEX", "VN30", "HNX", "HNX30", "UPCOM"]

SCHEMA_PATH = Path(__file__).resolve().parent.parent.parent / "schemas" / "market_index.avsc"

# ── Helpers ───────────────────────────────────────────────────────
def _load_avro_schema() -> str:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return f.read()


def _to_int(v) -> int | None:
    """Chuyển an toàn sang int, xử lý cả kiểu string trả về từ API."""
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None



def _mi_to_dict(mi: MarketIndex, _ctx) -> dict:
    # epoch float -> milliseconds int cho Avro timestamp-millis
    exchange_ms = int(mi.transactTime * 1000)          if mi.transactTime          else None
    dnse_ms     = int(mi.multicastReceiveTime * 1000)  if mi.multicastReceiveTime  else None
    producer_ms = int(mi.receivedAt * 1000)            if mi.receivedAt            else None

    # Fallback: neu transactTime la None, dung dnse_ms (cung NTP clock, dam bao exchange_ts <= dnse_ts)
    if exchange_ms is None:
        exchange_ms = dnse_ms or int(datetime.now(timezone.utc).timestamp() * 1000)

    def _f(v): return float(v) if v is not None else None

    return {
        "index_name":        mi.indexName,
        "market_id":         mi.marketId,
        "value":             _f(mi.valueIndexes),
        "prior_value":       _f(mi.priorValueIndexes),
        "highest_value":     _f(mi.highestValueIndexes),
        "lowest_value":      _f(mi.lowestValueIndexes),
        "changed_value":     _f(mi.changedValue),
        "changed_ratio":     _f(mi.changedRatio),
        "up_count":          _to_int(mi.fluctuationUpIssueCount),
        "down_count":        _to_int(mi.fluctuationDownIssueCount),
        "steady_count":      _to_int(mi.fluctuationSteadinessIssueCount),
        "upper_limit_count": _to_int(mi.fluctuationUpperLimitIssueCount),
        "lower_limit_count": _to_int(mi.fluctuationLowerLimitIssueCount),
        "up_volume":         _to_int(mi.fluctuationUpIssueVolume),
        "down_volume":       _to_int(mi.fluctuationDownIssueVolume),
        "steady_volume":     _to_int(mi.fluctuationSteadinessIssueVolume),
        "total_volume":      _to_int(mi.totalVolumeTraded),
        "total_value":       _f(mi.grossTradeAmount),
        "match_volume":      _to_int(mi.contauctAccTrdVol),
        "match_value":       _f(mi.contauctAccTrdVal),
        "deal_volume":       _to_int(mi.blkTrdAccTrdVol),
        "deal_value":        _f(mi.blkTrdAccTrdVal),
        "trading_session_id": str(mi.tradingSessionId) if mi.tradingSessionId is not None else None,
        "exchange_ts":          exchange_ms,
        "dnse_ts":              dnse_ms,
        "producer_ts":          producer_ms,
    }


# ── Kafka setup ───────────────────────────────────────────────────
def _create_producer():
    schema_registry  = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer  = AvroSerializer(
        schema_registry_client=schema_registry,
        schema_str=_load_avro_schema(),
        to_dict=_mi_to_dict,
    )
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 200,
        "compression.type": "lz4",
        "acks": "1",
    })
    key_serializer = StringSerializer("utf_8")
    return producer, avro_serializer, key_serializer


def _delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err} | topic={msg.topic()}")


# ── Main ──────────────────────────────────────────────────────────
async def main():
    producer, avro_serializer, key_serializer = _create_producer()
    msg_count = 0

    def handle_market_index(mi: MarketIndex):
        nonlocal msg_count
        try:
            key   = key_serializer(mi.indexName)
            value = avro_serializer(mi, SerializationContext(KAFKA_TOPIC, MessageField.VALUE))
            producer.produce(topic=KAFKA_TOPIC, key=key, value=value, on_delivery=_delivery_report)
            msg_count += 1
        except BufferError:
            producer.poll(0)
            try:
                producer.produce(topic=KAFKA_TOPIC, key=key, value=value, on_delivery=_delivery_report)
                msg_count += 1
            except BufferError:
                print(f"[WARN] Buffer full, dropping: {mi.indexName}")
        except Exception as e:
            print(f"[ERROR] Produce failed: {e}")

    encoding = "json"
    client = TradingClient(
        api_key=DNSE_API_KEY,
        api_secret=DNSE_API_SECRET,
        base_url=DNSE_WS_URL,
        encoding=encoding,
    )

    print(f"[START] Market Index Producer -> Kafka ({KAFKA_BOOTSTRAP})")
    print(f"[CONFIG] Topic: {KAFKA_TOPIC} | Indices: {MARKET_INDICES}")

    await client.connect()
    print("[SUCCESS] Connected to DNSE WebSocket!")

    for idx in MARKET_INDICES:
        await client.subscribe_market_index(
            market_index=idx,
            on_market_index=handle_market_index,
            encoding=encoding,
        )
    print(f"[SUBSCRIBED] Listening to {len(MARKET_INDICES)} market indices...")

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
