-- ============================================================
-- Bảng pipeline_config — Dynamic runtime configuration store
--
-- Key-value store cho phép thay đổi cấu hình pipeline
-- mà không cần restart container hay rebuild image.
--
-- Các service Python poll bảng này định kỳ:
--   - symbol_filter group: mỗi 60s (nhạy cảm giờ giao dịch)
--   - flush / connection groups: mỗi 300s
--
-- Thay đổi config qua Airflow DAG: dag_pipeline_config
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_config (
    key         TEXT        PRIMARY KEY,
    value       TEXT        NOT NULL,
    group_name  TEXT        NOT NULL DEFAULT 'default',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    description TEXT
);

COMMENT ON TABLE pipeline_config IS
    'Dynamic runtime config store. Polled by producers/consumers every 60-300s.';

COMMENT ON COLUMN pipeline_config.key IS
    'Config key, snake_case. E.g. symbol_filter_indexes, flush_interval_seconds.';

COMMENT ON COLUMN pipeline_config.group_name IS
    'Poll group: symbol_filter (60s), flush (300s), connection (300s).';

COMMENT ON COLUMN pipeline_config.updated_at IS
    'Last update timestamp. Automatically refreshed by trigger on UPDATE.';

-- ── Auto-update updated_at on every UPDATE ─────────────────────────────────
CREATE OR REPLACE FUNCTION _pipeline_config_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_pipeline_config_updated ON pipeline_config;
CREATE TRIGGER trg_pipeline_config_updated
    BEFORE UPDATE ON pipeline_config
    FOR EACH ROW
    EXECUTE FUNCTION _pipeline_config_set_updated_at();

-- ── Index để query nhanh theo group ───────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_pipeline_config_group
    ON pipeline_config (group_name, updated_at DESC);

-- ── Seed values — khớp với .env hiện tại ──────────────────────────────────
-- Chỉ insert nếu chưa có (ON CONFLICT DO NOTHING).
-- Để thay đổi: dùng Airflow DAG dag_pipeline_config, không sửa file này.
INSERT INTO pipeline_config (key, value, group_name, description) VALUES
  -- Symbol filter (poll every 60s)
  ('symbol_filter_mode',     'db',              'symbol_filter', 'Resolver mode: db or static'),
  ('symbol_filter_indexes',  'VN30,VN100,HNX30','symbol_filter', 'Comma-sep index names (OR logic). E.g. VN30,VN100,HNX30'),
  ('symbol_filter_groups',   'FU',              'symbol_filter', 'Comma-sep security_group_id to always include. E.g. FU'),
  ('symbol_filter_status',   'NO_HALT',         'symbol_filter', 'Comma-sep security_status values. E.g. NO_HALT'),
  ('symbol_filter_admin',    'NRM',             'symbol_filter', 'Comma-sep admin_status values. Empty = no filter'),
  ('symbol_filter_sanction', 'NRM',             'symbol_filter', 'Comma-sep trading_sanction_status. E.g. NRM'),
  ('symbol_filter_board_id', 'G1',              'symbol_filter', 'board_id filter. E.g. G1'),
  ('symbol_filter_market',   '',                'symbol_filter', 'Comma-sep market_id restriction. Empty = all markets'),

  -- Flush settings (poll every 300s)
  ('flush_batch_size',       '100',             'flush', 'Consumer batch size before forced flush'),
  ('flush_timeout_seconds',  '2.0',             'flush', 'Consumer batch timeout (seconds) before forced flush'),
  ('stats_flush_interval',   '30',              'flush', 'StatsReporter flush interval to pipeline_stats (seconds)'),

  -- Connection settings (poll every 300s)
  ('connection_timeout',     '5',               'connection', 'DB connect_timeout for config polling (seconds)')
ON CONFLICT (key) DO NOTHING;
