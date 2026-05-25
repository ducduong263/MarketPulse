import asyncio
import os
import random
import signal
import time
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

load_dotenv()

# -- Config ----------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("kafka_bootstrap_servers", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("schema_registry_url", "http://localhost:8081")
KAFKA_TOPIC = "market.orderbook-l2"

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "order_book_l2.avsc"


def _load_avro_schema() -> str:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return f.read()


# -- Mock Data Generator ---------------------------------------------------
def generate_mock_quote(symbol: str) -> dict:
    now_ms = int(time.time() * 1000)

    if symbol == "41I1G5000":  # Phái sinh - 10 levels
        base_price = 1250.0 + random.uniform(-2.0, 2.0)
        # Tạo 10 levels bid (giảm dần từ base_price)
        bids = [
            {"price": round(base_price - i * 0.1, 1), "qtty": random.randint(100, 500)}
            for i in range(1, 11)
        ]
        # Tạo 10 levels ask (tăng dần từ base_price)
        asks = [
            {"price": round(base_price + i * 0.1, 1), "qtty": random.randint(100, 500)}
            for i in range(1, 11)
        ]

        # Phái sinh thường trả về totalBidQtty / totalOfferQtty lớn hơn tổng các level hiển thị
        total_bid_qty = sum(b["qtty"] for b in bids) + random.randint(1000, 5000)
        total_ask_qty = sum(a["qtty"] for a in asks) + random.randint(1000, 5000)

        market_id = "DVX"
        board_id = "G1"

    else:  # Cổ phiếu (ACB) - 3 levels
        base_price = 27.5 + random.uniform(-0.3, 0.3)
        # Tạo 3 levels bid (giảm dần)
        bids = [
            {"price": round(base_price - i * 0.05, 2), "qtty": random.randint(1000, 10000)}
            for i in range(1, 4)
        ]
        # Tạo 3 levels ask (tăng dần)
        asks = [
            {"price": round(base_price + i * 0.05, 2), "qtty": random.randint(1000, 10000)}
            for i in range(1, 4)
        ]

        # Cổ phiếu HOSE không có totalBidQtty / totalOfferQtty
        total_bid_qty = None
        total_ask_qty = None

        market_id = "STO"
        board_id = "G1"

    # Trả về dict format khớp 100% với Avro schema (order_book_l2.avsc)
    return {
        "symbol": symbol,
        "market_id": market_id,
        "board_id": board_id,
        # Top 3 levels cho backward compatibility
        "bid_price1": bids[0]["price"],
        "bid_qty1": bids[0]["qtty"],
        "bid_price2": bids[1]["price"] if len(bids) > 1 else None,
        "bid_qty2": bids[1]["qtty"] if len(bids) > 1 else None,
        "bid_price3": bids[2]["price"] if len(bids) > 2 else None,
        "bid_qty3": bids[2]["qtty"] if len(bids) > 2 else None,
        
        "ask_price1": asks[0]["price"],
        "ask_qty1": asks[0]["qtty"],
        "ask_price2": asks[1]["price"] if len(asks) > 1 else None,
        "ask_qty2": asks[1]["qtty"] if len(asks) > 1 else None,
        "ask_price3": asks[2]["price"] if len(asks) > 2 else None,
        "ask_qty3": asks[2]["qtty"] if len(asks) > 2 else None,
        
        "total_bid_qty": total_bid_qty,
        "total_ask_qty": total_ask_qty,
        # Full depth arrays
        "bid_levels": bids,
        "ask_levels": asks,
        
        "exchange_ts": now_ms,
        "dnse_ts": now_ms + random.randint(5, 20),
        "producer_ts": now_ms + random.randint(20, 50),
    }


def _delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[SENT] {msg.key().decode('utf-8')} -> Partition {msg.partition()} | Offset {msg.offset()}")


# -- Main ------------------------------------------------------------------
async def main():
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry,
        schema_str=_load_avro_schema(),
    )

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 10,
        "acks": "1",
    })

    key_serializer = StringSerializer("utf_8")

    print(f"[START] Mock Quote Producer -> Kafka ({KAFKA_BOOTSTRAP})")
    print(f"[CONFIG] Target Topic: {KAFKA_TOPIC}")

    running = True

    def _signal_handler(sig, frame):
        nonlocal running
        print("\n[STOP] Shutting down mock producer...")
        running = False

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    symbols = ["ACB", "41I1G5000"]

    try:
        while running:
            for symbol in symbols:
                quote_data = generate_mock_quote(symbol)
                key = key_serializer(symbol)
                value = avro_serializer(
                    quote_data,
                    SerializationContext(KAFKA_TOPIC, MessageField.VALUE),
                )

                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=key,
                    value=value,
                    on_delivery=_delivery_report,
                )
            
            producer.poll(0)
            await asyncio.sleep(0.1)  # Gửi quote mới mỗi giây
            
    except Exception as e:
        print(f"[ERROR] Unexpected: {e}")
    finally:
        producer.flush(timeout=5)
        print("[DONE] Mock producer stopped.")


if __name__ == "__main__":
    asyncio.run(main())
