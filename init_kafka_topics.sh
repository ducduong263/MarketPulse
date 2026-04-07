#!/bin/bash

KAFKA="docker exec kafka kafka-topics"

echo "Creating Kafka topics..."

# Trade data (trade_extra channel) — partitioned by symbol
$KAFKA --create \
  --bootstrap-server localhost:29092 \
  --topic market.trade \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000

# Order book L2 snapshots (quote channel)
$KAFKA --create \
  --bootstrap-server localhost:29092 \
  --topic market.orderbook-l2 \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000

# News
$KAFKA --create \
  --bootstrap-server localhost:29092 \
  --topic news.raw \
  --partitions 1 \
  --replication-factor 1

$KAFKA --create \
  --bootstrap-server localhost:29092 \
  --topic news.sentiment \
  --partitions 1 \
  --replication-factor 1

echo "Topics created:"
$KAFKA --list --bootstrap-server localhost:29092