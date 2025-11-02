-- ============================================
-- TimescaleDB Configuration for Beverage Filling Line
-- ============================================

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================
-- Main Metrics Table
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

-- Convert to hypertable with 1-day chunks (better for high-frequency data)
SELECT create_hypertable('beverage_metrics', 'time', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

-- ============================================
-- Retention & Compression Policies
-- ============================================

-- Keep raw data for 90 days
SELECT add_retention_policy('beverage_metrics', 
    INTERVAL '90 days', 
    if_not_exists => TRUE);

-- Compress chunks older than 7 days (saves ~90% disk space)
ALTER TABLE beverage_metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'line, metric',
    timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('beverage_metrics', 
    INTERVAL '7 days',
    if_not_exists => TRUE);

-- ============================================
-- Indexes for Query Performance
-- ============================================

-- Composite index for most common Grafana queries (metric + line + time)
CREATE INDEX IF NOT EXISTS idx_metric_line_time 
    ON beverage_metrics (metric, line, time DESC);

-- Index for production order filtering
CREATE INDEX IF NOT EXISTS idx_production_order_time 
    ON beverage_metrics (production_order, time DESC) 
    WHERE production_order IS NOT NULL;

-- Index for article filtering
CREATE INDEX IF NOT EXISTS idx_article_time 
    ON beverage_metrics (article, time DESC) 
    WHERE article IS NOT NULL;

-- Index for text values (status, quality checks, etc.)
CREATE INDEX IF NOT EXISTS idx_text_value 
    ON beverage_metrics (metric, text_value, time DESC) 
    WHERE text_value IS NOT NULL;

-- Partial index for numeric values only
CREATE INDEX IF NOT EXISTS idx_numeric_values 
    ON beverage_metrics (metric, line, time DESC) 
    WHERE value IS NOT NULL;

-- ============================================
-- CONTINUOUS AGGREGATE: Hourly Process KPIs
-- (For numeric process values only)
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_process_kpis
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
    COUNT(*) as sample_count,
    -- Additional useful stats
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) as median_value,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY value) as p95_value
FROM beverage_metrics
WHERE value IS NOT NULL 
    AND metric LIKE 'process_%'  -- Only process metrics
GROUP BY hour, line, metric, production_order, article, machine_name, plant;

-- Refresh every 5 minutes, with only 15-minute lag (for near real-time dashboards)
SELECT add_continuous_aggregate_policy('hourly_process_kpis',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE);

-- ============================================
-- CONTINUOUS AGGREGATE: Hourly Counter KPIs
-- (For production counters)
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_counter_kpis
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    line,
    metric,
    production_order,
    article,
    machine_name,
    plant,
    MAX(value) - MIN(value) as count_delta,  -- Bottles produced in hour
    MAX(value) as max_count,
    MIN(value) as min_count,
    COUNT(*) as sample_count
FROM beverage_metrics
WHERE value IS NOT NULL 
    AND metric LIKE 'counters_%'  -- Only counter metrics
GROUP BY hour, line, metric, production_order, article, machine_name, plant;

-- Refresh every 5 minutes
SELECT add_continuous_aggregate_policy('hourly_counter_kpis',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE);

-- ============================================
-- CONTINUOUS AGGREGATE: Daily KPIs
-- (For historical analysis)
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
WHERE value IS NOT NULL
GROUP BY day, line, metric, production_order, article, machine_name, plant;

-- Refresh every hour
SELECT add_continuous_aggregate_policy('daily_kpis',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE);

-- Keep daily aggregates for 2 years
SELECT add_retention_policy('daily_kpis', 
    INTERVAL '2 years', 
    if_not_exists => TRUE);

-- ============================================
-- CONTINUOUS AGGREGATE: Production Order Summary
-- (Summary statistics per production order)
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS production_order_summary
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    production_order,
    article,
    line,
    machine_name,
    plant,
    MIN(time) as order_start_time,
    MAX(time) as order_last_update,
    -- Bottle counts
    MAX(value) FILTER (WHERE metric = 'counters_good_bottles_order') as good_bottles,
    MAX(value) FILTER (WHERE metric = 'counters_bad_bottles_order') as bad_bottles,
    MAX(value) FILTER (WHERE metric = 'counters_total_bottles_order') as total_bottles,
    -- Quality rate
    (MAX(value) FILTER (WHERE metric = 'counters_good_bottles_order')::float / 
     NULLIF(MAX(value) FILTER (WHERE metric = 'counters_total_bottles_order'), 0) * 100) as quality_rate,
    -- Average process values
    AVG(value) FILTER (WHERE metric = 'process_fill_volume_actual') as avg_fill_volume,
    AVG(value) FILTER (WHERE metric = 'process_line_speed_actual') as avg_line_speed,
    AVG(value) FILTER (WHERE metric = 'process_temperature_actual') as avg_temperature
FROM beverage_metrics
WHERE production_order IS NOT NULL
GROUP BY hour, production_order, article, line, machine_name, plant;

-- Refresh every 10 minutes
SELECT add_continuous_aggregate_policy('production_order_summary',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '10 minutes',
    if_not_exists => TRUE);

-- Keep order summaries for 1 year
SELECT add_retention_policy('production_order_summary', 
    INTERVAL '1 year', 
    if_not_exists => TRUE);

-- ============================================
-- CONTINUOUS AGGREGATE: Machine Status Timeline
-- (Downtime/error tracking)
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS machine_status_timeline
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time) AS time_bucket,
    line,
    machine_name,
    plant,
    production_order,
    -- Count occurrences of each status
    COUNT(*) FILTER (WHERE text_value = 'Running') as running_count,
    COUNT(*) FILTER (WHERE text_value = 'Error') as error_count,
    COUNT(*) FILTER (WHERE text_value = 'Stopped') as stopped_count,
    COUNT(*) FILTER (WHERE text_value = 'Maintenance') as maintenance_count,
    COUNT(*) FILTER (WHERE text_value = 'Starting') as starting_count,
    COUNT(*) FILTER (WHERE text_value = 'Stopping') as stopping_count,
    -- Most common status in this bucket
    MODE() WITHIN GROUP (ORDER BY text_value) as dominant_status,
    COUNT(*) as total_samples
FROM beverage_metrics
WHERE metric = 'machine_status' AND text_value IS NOT NULL
GROUP BY time_bucket, line, machine_name, plant, production_order;

-- Refresh every 5 minutes
SELECT add_continuous_aggregate_policy('machine_status_timeline',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE);

-- Keep status timeline for 6 months
SELECT add_retention_policy('machine_status_timeline', 
    INTERVAL '6 months', 
    if_not_exists => TRUE);

-- ============================================
-- Useful Views for Grafana
-- ============================================

-- Latest numeric metrics (for gauges and single-value displays)
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

-- Latest status values (for text metrics)
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

-- Current machine state (all important metrics in one view)
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

-- Active alarms view
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

-- ============================================
-- Helper Functions for Grafana
-- ============================================

-- Function to calculate uptime percentage
CREATE OR REPLACE FUNCTION calculate_uptime(
    p_line VARCHAR,
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
)
RETURNS NUMERIC AS $$
DECLARE
    total_samples INTEGER;
    running_samples INTEGER;
    uptime_pct NUMERIC;
BEGIN
    SELECT 
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE text_value = 'Running') as running
    INTO total_samples, running_samples
    FROM beverage_metrics
    WHERE line = p_line
        AND metric = 'machine_status'
        AND time BETWEEN p_start_time AND p_end_time;
    
    IF total_samples = 0 THEN
        RETURN 0;
    END IF;
    
    uptime_pct := (running_samples::NUMERIC / total_samples::NUMERIC) * 100;
    RETURN ROUND(uptime_pct, 2);
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- Documentation & Comments
-- ============================================

COMMENT ON TABLE beverage_metrics IS 'Time-series data for beverage filling line metrics with 1-day chunk intervals';
COMMENT ON COLUMN beverage_metrics.value IS 'Numeric values (temperatures, speeds, volumes, counts, etc.)';
COMMENT ON COLUMN beverage_metrics.text_value IS 'Text values (status, quality checks, alarms, etc.)';

COMMENT ON VIEW latest_metrics IS 'Most recent numeric value for each metric per line';
COMMENT ON VIEW latest_status IS 'Most recent text value for each metric per line';
COMMENT ON VIEW current_machine_state IS 'Current state of all machines with key metrics in one view';
COMMENT ON VIEW active_alarms IS 'Active alarms from the last 10 minutes';

COMMENT ON MATERIALIZED VIEW hourly_process_kpis IS 'Hourly aggregates for process metrics (refreshed every 5 min)';
COMMENT ON MATERIALIZED VIEW hourly_counter_kpis IS 'Hourly aggregates for counter metrics (refreshed every 5 min)';
COMMENT ON MATERIALIZED VIEW daily_kpis IS 'Daily aggregates for all numeric metrics (refreshed hourly)';
COMMENT ON MATERIALIZED VIEW production_order_summary IS 'Summary statistics per production order (refreshed every 10 min)';
COMMENT ON MATERIALIZED VIEW machine_status_timeline IS '5-minute status buckets for downtime analysis (refreshed every 5 min)';

-- ============================================
-- Grant permissions (optional - adjust as needed)
-- ============================================

-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_user;
-- GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA public TO grafana_user;