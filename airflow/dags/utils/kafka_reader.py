"""
utils/kafka_reader.py — Kafka read helper for MarketPulse DAGs.

Provides:
- read_kafka_from_timestamp(topic, start_ts, group_id, schema_path):
    Read all messages from a Kafka topic starting from a given UTC timestamp.
    Returns a list of dicts (Avro-deserialized messages).
"""

import os
import logging
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka import DeserializingConsumer

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP",      "kafka:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL",  "http://schema-registry:8081")

# How long to wait for new messages before assuming we've reached the end
EOF_TIMEOUT_S = 10.0


def read_kafka_from_timestamp(
    topic: str,
    start_ts: datetime,
    schema_str: str,
    group_id: str | None = None,
    end_ts: datetime | None = None,
) -> list[dict]:
    """
    Read all Kafka messages from `topic` starting from `start_ts` (inclusive).
    Stops when no new messages arrive for EOF_TIMEOUT_S seconds or `end_ts` is reached.

    Uses offsets_for_times() to seek to the exact timestamp — avoids reading
    from the beginning of the topic.

    Args:
        topic:      Kafka topic name, e.g. "market.foreign-investor"
        start_ts:   UTC datetime — seek to this point in the topic
        schema_str: Avro schema string for deserialization
        group_id:   Consumer group ID (default: auto-generated "eod-reader-<topic>")
        end_ts:     Optional UTC datetime — stop reading after this timestamp

    Returns:
        List of dicts (deserialized Avro records), in offset order.
    """
    import time

    if group_id is None:
        group_id = f"eod-reader-{topic.replace('.', '-')}"

    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(sr_client, schema_str)

    consumer = DeserializingConsumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           group_id,
        "auto.offset.reset":  "earliest",
        "value.deserializer": avro_deserializer,
        "enable.auto.commit": False,
    })

    # Get all partitions for this topic
    metadata = consumer.list_topics(topic, timeout=10)
    if topic not in metadata.topics:
        logger.warning(f"Topic {topic} not found")
        consumer.close()
        return []

    partitions = [
        TopicPartition(topic, pid)
        for pid in metadata.topics[topic].partitions.keys()
    ]

    # Seek each partition to the start timestamp
    start_ms = int(start_ts.timestamp() * 1000)
    ts_partitions = [TopicPartition(topic, p.partition, start_ms) for p in partitions]
    offsets = consumer.offsets_for_times(ts_partitions, timeout=10)

    # Assign only partitions that have data at/after start_ts
    valid = []
    for op in offsets:
        if op.offset >= 0:
            valid.append(op)
        else:
            logger.info(f"  Partition {op.partition}: no messages at/after {start_ts.isoformat()}")

    if not valid:
        logger.info(f"No messages found in {topic} from {start_ts.isoformat()}")
        consumer.close()
        return []

    consumer.assign(valid)

    records = []
    last_msg_time = time.monotonic()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message — check idle timeout
                if time.monotonic() - last_msg_time > EOF_TIMEOUT_S:
                    logger.info(f"[EOF] No new messages for {EOF_TIMEOUT_S}s — done reading {topic}")
                    break
                continue

            err = msg.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {err}")
                continue

            record = msg.value()
            if record is None:
                continue

            # Optionally filter by end_ts using message timestamp
            if end_ts is not None:
                msg_ts_ms = msg.timestamp()[1]  # (type, timestamp_ms)
                if msg_ts_ms > int(end_ts.timestamp() * 1000):
                    continue

            records.append(record)
            last_msg_time = time.monotonic()

    finally:
        consumer.close()

    logger.info(f"[KAFKA] Read {len(records)} records from {topic} (from {start_ts.isoformat()})")
    return records
