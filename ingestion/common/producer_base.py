"""
ingestion/common/producer_base.py

Base class for all DNSE WebSocket -> Kafka Avro producers.

Handles:
- Kafka Producer + AvroSerializer setup
- Message produce with BufferError retry
- Async event loop with graceful shutdown (SIGINT/SIGTERM)
- producer.flush() on exit
- StatsReporter: periodic flush of health metrics to pipeline_stats table
- Hot-reload: periodic symbol set check — subscribes new symbols without restart
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

from .stats_reporter import StatsReporter

DNSE_WS_URL = "wss://ws-openapi.dnse.com.vn"

# How often (seconds) to re-check symbol config for hot-reload
SYMBOL_RELOAD_INTERVAL = 60

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
      - topic:           Kafka topic name
      - schema_path:     Path to .avsc schema file
      - to_dict_fn:      Function (message, ctx) -> dict for AvroSerializer
      - producer_config: Optional overrides for Kafka producer settings
      - service_name:    Name used for pipeline_stats (default: class name)
      - symbol_resolver: Optional SymbolResolver instance for hot-reload.
                         If provided, run() will check for new symbols every
                         SYMBOL_RELOAD_INTERVAL seconds and call
                         resubscribe_fn(client, new_symbols) if any are found.
      - resubscribe_fn:  async (client, new_symbols: list[str]) -> None
                         Called with only the NEW symbols to add.
                         Required if symbol_resolver is provided.

    Usage:
        producer = DnseKafkaProducer(
            topic=KAFKA_TOPIC,
            schema_path=SCHEMA_PATH,
            to_dict_fn=_trade_to_dict,
            producer_config={"linger.ms": 50, "batch.num.messages": 500},
            service_name="p-trade",
            symbol_resolver=resolver,
            resubscribe_fn=resubscribe_fn,
        )
        asyncio.run(producer.run(subscribe_fn))
    """

    def __init__(
        self,
        topic: str,
        schema_path: Path,
        to_dict_fn: Callable,
        producer_config: dict | None = None,
        service_name: str | None = None,
        symbol_resolver=None,
        resubscribe_fn: Callable | None = None,
    ) -> None:
        self.topic = topic
        self._schema_path = schema_path
        self._to_dict_fn = to_dict_fn
        self._symbol_resolver = symbol_resolver
        self._resubscribe_fn = resubscribe_fn

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

        # ── Stats reporter ─────────────────────────────────────────
        _svc = service_name or self.__class__.__name__.lower()
        self._stats = StatsReporter(service_name=_svc)

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
            self._stats.inc_avro_error()
            return

        try:
            self._producer.produce(
                topic=self.topic, key=k, value=v,
                on_delivery=self._delivery_report,
            )
            self._msg_count += 1
            self._stats.inc_msg()

        except BufferError:
            self._producer.poll(0)
            try:
                self._producer.produce(
                    topic=self.topic, key=k, value=v,
                    on_delivery=self._delivery_report,
                )
                self._msg_count += 1
                self._stats.inc_msg()
            except BufferError:
                print(f"[WARN] Buffer full after poll, dropping: {key}")

        except Exception as e:
            print(f"[ERROR] Produce failed for {key}: {e}")

    async def run(self, subscribe_fn: Callable) -> None:
        """
        Full async lifecycle:
          1. Build TradingClient from env vars
          2. connect()
          3. Call subscribe_fn(client) — caller sets up initial subscription
          4. Poll loop (asyncio.sleep 0.1s) with periodic symbol hot-reload
          5. Graceful shutdown on SIGINT/SIGTERM
          6. producer.flush()
          7. StatsReporter.stop() — final metrics flush

        Hot-reload:
          If symbol_resolver + resubscribe_fn are provided, checks every
          SYMBOL_RELOAD_INTERVAL seconds for new symbols in config.
          Calls resubscribe_fn(client, new_symbols) for additions only.
          Existing subscriptions are never dropped (SDK limitation).

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

        # ── Start stats reporter before connecting ─────────────────
        self._stats.start()
        self._stats.mark_offline()  # service starting up, not yet connected

        await client.connect()
        print("[CONNECTED] DNSE WebSocket connected")
        self._stats.mark_online()  # first successful connection

        # ── Register reconnect event handlers ──────────────────────
        client.on("reconnecting", lambda data: self._stats.mark_offline())
        client.on("reconnected", lambda data: self._stats.mark_online())

        await subscribe_fn(client)

        # ── Track subscribed symbol set for hot-reload ─────────────
        _current_symbols: set[str] = set()
        if self._symbol_resolver is not None:
            try:
                _current_symbols = set(self._symbol_resolver.resolve())
            except Exception:
                pass
        _last_reload = asyncio.get_event_loop().time()

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

                # ── Hot-reload: check for new symbols every 60s ────
                if (
                    self._symbol_resolver is not None
                    and self._resubscribe_fn is not None
                    and (loop.time() - _last_reload) >= SYMBOL_RELOAD_INTERVAL
                ):
                    _last_reload = loop.time()
                    try:
                        new_symbols = self._symbol_resolver.resolve_new_symbols(_current_symbols)
                        if new_symbols:
                            print(
                                f"[HOT-RELOAD] {len(new_symbols)} new symbol(s) detected: "
                                f"{sorted(new_symbols)}"
                            )
                            await self._resubscribe_fn(client, sorted(new_symbols))
                            _current_symbols.update(new_symbols)
                            print(
                                f"[HOT-RELOAD] Subscribed. Total symbols: {len(_current_symbols)}"
                            )
                    except Exception as e:
                        print(f"[HOT-RELOAD][WARN] Symbol reload check failed: {e}")

        except KeyboardInterrupt:
            print("\n[STOP] Shutting down...")
        finally:
            self._stats.mark_offline()       # mark service going offline
            self._stats.stop()  # final metrics flush before exit
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
