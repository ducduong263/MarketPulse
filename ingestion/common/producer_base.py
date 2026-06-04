"""
ingestion/common/producer_base.py

Base class for all DNSE WebSocket -> Kafka Avro producers.

Handles:
- Kafka Producer + AvroSerializer setup
- Message produce with BufferError retry
- Async event loop with graceful shutdown (SIGINT/SIGTERM)
- producer.flush() on exit
"""
from __future__ import annotations

import asyncio
import os
import signal
from pathlib import Path
from typing import Callable

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

DNSE_WS_URL = "wss://ws-openapi.dnse.com.vn"

_DEFAULT_PRODUCER_CONFIG = {
    "linger.ms": 50,
    "batch.num.messages": 500,
    "compression.type": "lz4",
    "acks": "1",
}


class DnseKafkaProducer:
    """
    Reusable DNSE WebSocket -> Kafka Avro producer base.

    Each concrete producer provides:
      - topic:         Kafka topic name
      - schema_path:   Path to .avsc schema file
      - to_dict_fn:    Function (message, ctx) -> dict for AvroSerializer
      - producer_config: Optional overrides for Kafka producer settings

    Usage:
        producer = DnseKafkaProducer(
            topic=KAFKA_TOPIC,
            schema_path=SCHEMA_PATH,
            to_dict_fn=_trade_to_dict,
            producer_config={"linger.ms": 50, "batch.num.messages": 500},
        )
        asyncio.run(producer.run(subscribe_fn))
    """

    def __init__(self, topic: str, schema_path: Path, to_dict_fn: Callable, producer_config: dict | None = None,) -> None:
        self.topic = topic
        self._schema_path = schema_path
        self._to_dict_fn = to_dict_fn

        kafka_bootstrap = os.getenv("kafka_bootstrap_servers", "localhost:9092")
        schema_registry_url = os.getenv("schema_registry_url", "http://localhost:8081")

        schema_str = schema_path.read_text(encoding="utf-8")
        sr = SchemaRegistryClient({"url": schema_registry_url})
        self._avro_serializer = AvroSerializer(
            schema_registry_client=sr,
            schema_str=schema_str,
            to_dict=to_dict_fn,
        )

        config = {**_DEFAULT_PRODUCER_CONFIG, **(producer_config or {})}
        config["bootstrap.servers"] = kafka_bootstrap
        self._producer = Producer(config)
        self._key_serializer = StringSerializer("utf_8")
        self._msg_count = 0

    # ── Public API ────────────────────────────────────────────────

    def produce(self, key: str, message) -> None:
        """
        Serialize and produce a message to Kafka.
        Retries once after poll() on BufferError; drops if still full.
        """
        try:
            k = self._key_serializer(key)
            v = self._avro_serializer(
                message,
                SerializationContext(self.topic, MessageField.VALUE),
            )
        except Exception as e:
            print(f"[ERROR] Serialization failed for {key}: {e}")
            return

        try:
            self._producer.produce(
                topic=self.topic, key=k, value=v,
                on_delivery=self._delivery_report,
            )
            self._msg_count += 1

        except BufferError:
            self._producer.poll(0)
            try:
                self._producer.produce(
                    topic=self.topic, key=k, value=v,
                    on_delivery=self._delivery_report,
                )
                self._msg_count += 1
            except BufferError:
                print(f"[WARN] Buffer full after poll, dropping: {key}")

        except Exception as e:
            print(f"[ERROR] Produce failed for {key}: {e}")

    async def run(self, subscribe_fn: Callable) -> None:
        """
        Full async lifecycle:
          1. Build TradingClient from env vars
          2. connect()
          3. Call subscribe_fn(client) — caller sets up subscription
          4. Poll loop (asyncio.sleep 0.1s)
          5. Graceful shutdown on SIGINT/SIGTERM
          6. producer.flush()

        Args:
            subscribe_fn: async coroutine, signature: async (client) -> None
        """
        from dnse import TradingClient

        client = TradingClient(
            api_key=os.environ["DNSE_API_KEY"],
            api_secret=os.environ["DNSE_API_SECRET"],
            base_url=DNSE_WS_URL,
            encoding="msgpack",
        )

        print(f"[START] {self.__class__.__name__} -> Kafka | topic={self.topic}")
        await client.connect()
        print("[CONNECTED] DNSE WebSocket connected")

        await subscribe_fn(client)

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
                self._producer.poll(0)
                await asyncio.sleep(0.1)
        except KeyboardInterrupt:
            print("\n[STOP] Shutting down...")
        finally:
            remaining = self._producer.flush(timeout=10)
            print(f"[DONE] Total: {self._msg_count} messages | Remaining in buffer: {remaining}")
            try:
                await client.disconnect()
            except Exception:
                pass

    # ── Internal ──────────────────────────────────────────────────

    def _delivery_report(self, err, msg) -> None:
        if err is not None:
            print(f"[ERROR] Delivery failed: {err} | topic={msg.topic()} key={msg.key()}")
