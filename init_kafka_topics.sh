#!/bin/bash

KAFKA="docker exec kafka kafka-topics"

echo "Creating Kafka topics..."

$KAFKA --create \
  --bootstrap-server localhost:29092 \
  --topic market.tick \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000

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