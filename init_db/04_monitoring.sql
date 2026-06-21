CREATE TABLE IF NOT EXISTS pipeline_stats (
    ts              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    service_name    TEXT            NOT NULL,
    metric_name     TEXT            NOT NULL,
    metric_value    DOUBLE PRECISION NOT NULL,
    label           TEXT
);

SELECT create_hypertable(
    'pipeline_stats',
    'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT add_retention_policy(
    'pipeline_stats',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_pipeline_stats_service_metric
    ON pipeline_stats (service_name, metric_name, ts DESC);

-- ── Comment ───────────────────────────────────────────────────────────────
COMMENT ON TABLE pipeline_stats IS
    'Realtime producer/consumer health metrics. Written every 30s by each service.';

COMMENT ON COLUMN pipeline_stats.service_name IS
    'Container name: p-trade, c-trade, p-quote, c-quote, p-index, c-index, etc.';

COMMENT ON COLUMN pipeline_stats.metric_name IS
    'One of: ws_connected, reconnect_count, msg_per_sec, avro_error_count, consumer_lag';

COMMENT ON COLUMN pipeline_stats.label IS
    'Optional tag: Kafka topic name, symbol name, consumer group name, etc.';
