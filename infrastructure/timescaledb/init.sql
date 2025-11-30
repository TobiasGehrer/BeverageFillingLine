CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================
-- Hypertable - Main Metrics Table
-- ============================================
CREATE TABLE IF NOT EXISTS beverage_metrics (
    time TIMESTAMPTZ NOT NULL,
    metric VARCHAR(100) NOT NULL,
    line VARCHAR(50) NOT NULL,
    value DOUBLE PRECISION,
    text_value TEXT,
    production_order VARCHAR(100),
    article VARCHAR(100),
    machine_name VARCHAR(100),
    plant VARCHAR(100)
);

-- Convert to hypertable (TimescaleDB partitioning by time)
SELECT create_hypertable('beverage_metrics', 'time', if_not_exists => TRUE);

-- ============================================
-- Retention Policy - Keep data for 90 days
-- ============================================
SELECT add_retention_policy('beverage_metrics', INTERVAL '90 days', if_not_exists => TRUE);

-- ============================================
-- Indexes for Performance
-- ============================================
CREATE INDEX IF NOT EXISTS idx_metric_line_time 
    ON beverage_metrics (metric, line, time DESC);

CREATE INDEX IF NOT EXISTS idx_production_order 
    ON beverage_metrics (production_order, time DESC) 
    WHERE production_order IS NOT NULL;

-- ============================================
-- Continuous Aggregate - Hourly KPIs
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_kpis
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    line,
    metric,
    production_order,
    article,
    machine_name,
    plant,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    STDDEV(value) as stddev_value,
    COUNT(*) as sample_count
FROM beverage_metrics
WHERE value IS NOT NULL  -- Only aggregate numeric values
GROUP BY hour, line, metric, production_order, article, machine_name, plant;

-- Refresh policy for hourly aggregates (every 30 minutes)
SELECT add_continuous_aggregate_policy('hourly_kpis',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists => TRUE);

-- ============================================
-- Continuous Aggregate - Daily KPIs
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_kpis
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS day,
    line,
    metric,
    production_order,
    article,
    machine_name,
    plant,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    STDDEV(value) as stddev_value,
    COUNT(*) as sample_count
FROM beverage_metrics
WHERE value IS NOT NULL  -- Only aggregate numeric values
GROUP BY day, line, metric, production_order, article, machine_name, plant;

-- Refresh policy for daily aggregates (every 6 hours)
SELECT add_continuous_aggregate_policy('daily_kpis',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '6 hours',
    if_not_exists => TRUE);

-- ============================================
-- Views for Grafana Dashboards
-- ============================================

-- Latest numeric value for each metric (for gauges)
CREATE OR REPLACE VIEW latest_metrics AS
SELECT DISTINCT ON (line, metric)
    time,
    line,
    metric,
    value,
    production_order,
    article,
    machine_name,
    plant
FROM beverage_metrics
WHERE value IS NOT NULL
ORDER BY line, metric, time DESC;

-- Latest text value for each metric (for status displays)
CREATE OR REPLACE VIEW latest_status AS
SELECT DISTINCT ON (line, metric)
    time,
    line,
    metric,
    text_value,
    production_order,
    article,
    machine_name,
    plant
FROM beverage_metrics
WHERE text_value IS NOT NULL
ORDER BY line, metric, time DESC;

-- Current machine state (all key metrics in one view)
CREATE OR REPLACE VIEW current_machine_state AS
SELECT 
    l.line,
    l.machine_name,
    l.plant,
    l.production_order,
    l.article,
    MAX(l.time) as last_update,
    MAX(l.value) FILTER (WHERE l.metric = 'process_fill_volume_actual') as fill_volume,
    MAX(l.value) FILTER (WHERE l.metric = 'process_line_speed_actual') as line_speed,
    MAX(l.value) FILTER (WHERE l.metric = 'process_temperature_actual') as temperature,
    MAX(l.value) FILTER (WHERE l.metric = 'process_tank_level_percent') as tank_level,
    MAX(l.value) FILTER (WHERE l.metric = 'counters_good_bottles') as good_bottles,
    MAX(l.value) FILTER (WHERE l.metric = 'counters_bad_bottles_total') as bad_bottles,
    MAX(s.text_value) FILTER (WHERE s.metric = 'machine_status') as machine_status,
    MAX(s.text_value) FILTER (WHERE s.metric = 'quality_weight_check') as quality_weight,
    MAX(s.text_value) FILTER (WHERE s.metric = 'quality_level_check') as quality_level
FROM beverage_metrics l
LEFT JOIN beverage_metrics s ON s.line = l.line AND s.text_value IS NOT NULL
WHERE l.time > NOW() - INTERVAL '5 minutes'
GROUP BY l.line, l.machine_name, l.plant, l.production_order, l.article;

-- Active alarms
CREATE OR REPLACE VIEW active_alarms AS
SELECT 
    time,
    line,
    machine_name,
    plant,
    production_order,
    text_value as alarm_text
FROM beverage_metrics
WHERE metric = 'alarms_active' 
    AND text_value IS NOT NULL
    AND time > NOW() - INTERVAL '10 minutes'
ORDER BY time DESC;