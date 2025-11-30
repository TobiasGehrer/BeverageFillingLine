# Beverage Filling Line - Unified Namespace

This project implements an industrial IoT data pipeline for a beverage filling line machine. The system continuously collects operational metrics (fill volume, line speed, CO2 pressure, cycle time, etc.) from an OPC-UA server, enriches them with contextual information from Redis, processes them through a message queue, and stores them in a time-series database for real-time and historical analysis.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Pipeline Components](#pipeline-components)
   - [OPC-UA Simulation Server](#opc-ua-simulation-server)
   - [Data Pipeline](#data-pipeline)
   - [Message Brokers & Enrichment](#message-brokers--enrichment)
   - [TimescaleDB](#timescaledb)
   - [Grafana Dashboards](#grafana-dashboards)
3. [Use Case: Data Enrichment & Hydration](#use-case-data-enrichment--hydration)
4. [Testing Each Component](#testing-each-component)
5. [Service Ports & Access](#service-ports--access)

---

## Getting Started

1. **Start all services:**
   ```bash
   docker-compose up -d
   ```

   This will:
   - Build the OPC-UA server and bridge applications
   - Start all containers (servers, brokers, databases, dashboards)
   - Automatically configure datasources and dashboards

2. **Verify all services are running:**
   ```bash
   docker-compose ps
   ```

   You should see 12 containers running:
   - opc-server
   - opc-mqtt-bridge
   - mqtt (mosquitto)
   - redis
   - redis_hydration
   - redpanda
   - redpanda-console
   - mqtt_to_kafka
   - kafka-timescaledb-bridge
   - timescaledb
   - pgadmin
   - grafana

---

## Pipeline Components

### OPC-UA Simulation Server

**Purpose:** Simulates a real beverage filling line machine with realistic operational data.

**Port:** 4840

**Implemented Fields:**
- **Machine Status:** Running, Stopped, Error, Maintenance
- **Current Station:** Station 1-5
- **Process Metrics:**
  - Fill Volume (ml) - target & actual
  - Line Speed (bottles/min) - target & actual
  - CO2 Pressure (bar) - target & actual
  - Cycle Time (seconds) - target & actual
  - Cap Torque (Nm) - target & actual
  - Tank Level (%)
  - Temperature (°C)

- **Production Data:**
  - Production Order, Article, Lot Number
  - Quantity, Progress %
  - Good/Bad bottles counters

- **Quality Checks:**
  - Fill Volume deviation
  - Weight check
  - Cap torque verification
  - Active alarms

**Testing:**
```bash
# Check if server is running and accessible
opc.tcp://localhost:4840

# View logs
docker logs opc-server
```

---

### Data Pipeline

The pipeline consists of 4 integrated components working together:

#### 1. OPC-UA to MQTT Bridge

**Component:** `src/OpcMqttBridge`

**What it does:**
- Connects to OPC-UA server on port 4840
- Reads all machine fields every 500ms
- Converts OPC-UA data to MQTT format
- Publishes to MQTT broker

**MQTT Topics:**
```
v1/best-beverage/line/metric_name/value
Example: v1/best-beverage/filling-line-1/process_fill_volume_actual/1003.5
```

**Testing:**
```bash
# Subscribe to MQTT topics and watch data flow
docker exec -it mosquitto-broker mosquitto_sub -h localhost -t "v1/best-beverage/#" -v

# View logs
docker logs opc-mqtt-bridge
```

#### 2. MQTT Broker (Mosquitto)

**Port:** 1883

**Purpose:** Central message hub for initial data distribution

**Features:**
- Allows anonymous connections
- Stores messages for subscribers
- Routes data to 2 downstream agents simultaneously

**Testing:**
```bash
# Check broker connectivity
docker exec -it mosquitto-broker mosquitto_sub -h localhost -t "\$SYS/#" -v
```

#### 3. Hydration Agent (Redis Enrichment)

**Component:** `config/redis_hydration.yaml` (Redpanda Connect)

**What it does:**
- Subscribes to MQTT topics
- Stores latest metric values in Redis for fast lookup
- Maintains current machine state in Redis hash

**How it works:**
```
MQTT Input (e.g., machine_status: "Running")
    ↓
Extract field name and value
    ↓
Store in Redis hash "filling-line-1"
    ↓
Result: HSET filling-line-1 machine_status "Running"
```

**Redis Storage Structure:**
```
Key: "filling-line-1"
Hash Fields (examples):
  - machine_status: "Running"
  - process_fill_volume_actual: "1003.5"
  - process_line_speed_actual: "450"
  - counters_good_bottles: "1247828"
  ... (all 76 metrics)
```

**Testing:**
```bash
# Connect to Redis and inspect data
docker exec -it redis redis-cli

# View all keys
> KEYS *
> HGETALL filling-line-1

# Get specific field
> HGET filling-line-1 machine_status

# View logs
docker logs redis_hydration
```

---

#### 4. MQTT to Kafka Bridge + Kafka to TimescaleDB Bridge

**Components:** 
- `config/mqtt_to_kafka.yaml` (Redpanda Connect) - MQTT → Kafka
- `src/KafkaTimescaledbBridge` (C# Application) - Kafka → TimescaleDB

**What they do:**
1. **MQTT to Kafka:**
   - Subscribes to MQTT topics
   - Enriches each message with Redis context data (production order, article, plant)
   - Publishes enriched events to Kafka topic: `beverage_metrics`

2. **Kafka to TimescaleDB:**
   - Consumes messages from Kafka queue
   - Writes to `beverage_metrics` hypertable
   - Validates data and handles failures gracefully

**Kafka Topic:** `beverage_metrics`

**Testing:**
```bash
# View messages in Kafka topic via Redpanda Console
http://localhost:8080

# Or use Kafka CLI
docker exec -it redpanda rpk topic consume beverage_metrics -n 10

# View logs
docker logs mqtt_to_kafka
docker logs kafka-timescaledb-bridge
```

---

### TimescaleDB

**Port:** 5432

**Database:** `beverage_data`

**Credentials:** admin / admin123

#### Hypertable: `beverage_metrics`

Stores all raw metric data with automatic time-based partitioning.

**Schema:**
```sql
CREATE TABLE beverage_metrics (
    time TIMESTAMPTZ NOT NULL,              -- Timestamp
    metric VARCHAR(100) NOT NULL,           -- Metric name (e.g., "process_fill_volume_actual")
    line VARCHAR(50) NOT NULL,              -- Line identifier (e.g., "filling-line-1")
    value DOUBLE PRECISION,                 -- Numeric value
    text_value TEXT,                        -- Text value (e.g., machine_status = "Running")
    production_order VARCHAR(100),          -- Production order ID (enriched)
    article VARCHAR(100),                   -- Product article (enriched)
    machine_name VARCHAR(100),              -- Machine name (enriched)
    plant VARCHAR(100)                      -- Plant location (enriched)
);
```

**Indexes:**
- `idx_metric_line_time` - Optimized for metric queries
- `idx_production_order` - Track production orders

**Data Retention:** 90 days (automatic cleanup)

#### Continuous Aggregate: `hourly_kpis`

Automatically computes hourly statistics for efficient long-term analysis.

**Query:**
```sql
SELECT
    time_bucket('1 hour', time) AS hour,
    line,
    metric,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    STDDEV(value) as stddev_value
FROM beverage_metrics
GROUP BY hour, line, metric
```

**Refresh Schedule:** Continuous (automatically updated as data arrives)

**Testing:**
```bash
# Connect to database
docker exec -it timescaledb psql -U admin -d beverage_data

# View raw metrics count
> SELECT COUNT(*) FROM beverage_metrics;

# View hourly aggregates
> SELECT * FROM hourly_kpis LIMIT 10;

# Manually refresh aggregates (if needed)
> CALL refresh_continuous_aggregate('hourly_kpis', NULL, NULL);

# View data distribution
> SELECT metric, COUNT(*) FROM beverage_metrics GROUP BY metric ORDER BY COUNT(*) DESC;
```

---

### Grafana Dashboards

**Port:** 3000

**Credentials:** admin / admin123

#### Short-Term Dashboard (24 Hours)

**Purpose:** Monitor current machine behavior and immediate issues

**Panels:**
1. **Line Speed (Last 24h)** - Time series graph from TimescaleDB
2. **Tank Level** - Current gauge from Redis (live value)
3. **CO2 Pressure (Last 24h)** - Time series graph from TimescaleDB
4. **Machine Status** - Current status from Redis (live value)
5. **Cycle Time (Last 24h)** - Time series graph from TimescaleDB
6. **Fill Volume Deviation (Last 24h)** - Time series graph from TimescaleDB
7. **Cap Torque (Last 24h)** - Time series graph from TimescaleDB
8. **Active Alarms** - Current alarms from Redis (live value)

**Data Sources:**
- **TimescaleDB** (5 panels) - Historical time-series data
- **Redis** (3 panels) - Current machine state

**Refresh Rate:** 5 seconds

#### Long-Term Dashboard (30 Days)

**Purpose:** Identify patterns, trends, and historical performance

**Panels:**
1. **Average Line Speed (30 Days)** - Hourly aggregates trend
2. **Daily Production (30 Days)** - Total bottles produced per day
3. **CO2 Pressure Trends (30 Days)** - Pressure patterns
4. **Fill Volume Deviation (30 Days)** - Quality consistency
5. **Bad Bottles by Type (30 Days)** - Defect breakdown
6. **Production Efficiency (30 Days)** - Good/total ratio percentage
7. **Cycle Time Statistics (30 Days)** - Performance trends
8. **Daily Alarm Count (30 Days)** - Issue frequency

**Data Source:** TimescaleDB continuous aggregates (`hourly_kpis`)

**Testing:**
```bash
# Navigate to Grafana
http://localhost:3000

# If no data appears in long-term dashboard:
# 1. Change time range to "Last 24h" to see recent data
# 2. Or refresh continuous aggregates:
docker exec -it timescaledb psql -U admin -d beverage_data \
  -c "CALL refresh_continuous_aggregate('hourly_kpis', NULL, NULL);"
```

---

## Use Case: Data Enrichment & Hydration

### The Problem

Raw OPC-UA data contains only machine metrics. To understand production context, we need to know:
- Which production order is running?
- What article (product) is being produced?
- Which plant location is this machine in?
- Who is the machine manufacturer?

Adding this information to every metric makes queries easier and dashboards more meaningful.

### The Solution: Hydration Pipeline

**Step 1: Store Context in Redis**

When a production order is loaded (e.g., via OPC-UA method call), the hydration agent stores these metadata in Redis:

```bash
HSET filling-line-1 \
  production_order "PO-2024-JUICE-5567" \
  production_article "ART-JUICE-APPLE-1L" \
  machine_name "FluidFill Express #2" \
  machine_plant "Dortmund Beverage Center"
```

**Step 2: Enrich Incoming Metrics**

Every metric that flows through MQTT gets enriched:

```yaml
# MQTT to Kafka Bridge Configuration (config/mqtt_to_kafka.yaml)

pipeline:
  processors:
    - branch:
        # For certain metrics, fetch context from Redis
        request_map: 'root = if this.metric.re_match("(machine_status|production_.*|quality_.*|counters_.*|process_.*)") { this.line }'
        processors:
          - redis:
              url: "redis://redis:6379"
              command: hmget
              args_mapping: |
                root = [ 
                  this,
                  "production_order",
                  "production_article",
                  "machine_name",
                  "machine_plant"
                ]
        result_map: |
          # Add fetched context to the message
          root.production_order = this.0
          root.article = this.1
          root.machine_name = this.2
          root.plant = this.3
```

**Step 3: Store Enriched Data**

The enriched message is published to Kafka and consumed by the C# bridge:

```json
{
  "timestamp": "2025-01-15T10:30:45.123Z",
  "metric": "process_fill_volume_actual",
  "line": "filling-line-1",
  "value": 1003.5,
  "production_order": "PO-2024-JUICE-5567",
  "article": "ART-JUICE-APPLE-1L",
  "machine_name": "FluidFill Express #2",
  "plant": "Dortmund Beverage Center"
}
```

This gets inserted into TimescaleDB's `beverage_metrics` table.

### Why This Matters

**Without Enrichment:**
```sql
SELECT * FROM beverage_metrics WHERE metric = 'process_fill_volume_actual';
-- Result: 1000s of rows with just timestamps and values
-- Question: Which rows belong to which production order?
-- Answer: Can't tell without joining external data
```

**With Enrichment:**
```sql
SELECT * FROM beverage_metrics 
WHERE metric = 'process_fill_volume_actual' 
  AND production_order = 'PO-2024-JUICE-5567';
-- Result: Only rows for this specific order
-- Instant insights without complex joins
```

**Benefits:**
- Faster queries (context already in the table)
- Easier analytics & reporting
- Better dashboard visualizations
- Production traceability built-in
- Compliance & audit trails

---

## Testing Each Component

### 1. Test OPC-UA Server

```bash
# Check server logs
docker logs opc-server

# Expected: "Server started at: opc.tcp://localhost:4840"

# Verify connectivity (requires OPC-UA client)
# Or check the bridge logs below
```

### 2. Test OPC-UA to MQTT Bridge

```bash
# View logs
docker logs opc-mqtt-bridge

# Expected: "Connected to OPC-UA server" and "Publishing to MQTT"

# Monitor MQTT traffic
docker exec -it mosquitto-broker mosquitto_sub -h localhost -t "v1/best-beverage/#" -v

# Expected: Messages flowing every 500ms with machine metrics
```

### 3. Test Redis Hydration

```bash
# Connect to Redis
docker exec -it redis redis-cli

# Check stored data
> HGETALL filling-line-1

# Check specific fields
> HGET filling-line-1 machine_status
> HGET filling-line-1 production_order

# Expected: ~76 fields with current machine state
```

### 4. Test Kafka Pipeline

```bash
# View Redpanda Console
http://localhost:8080

# Or consume from Kafka topic
docker exec -it redpanda rpk topic consume beverage_metrics -n 20

# Expected: JSON messages with enriched data (includes production_order, article, etc.)
```

### 5. Test TimescaleDB

```bash
# Connect to database
docker exec -it timescaledb psql -U admin -d beverage_data

# Check row count (should increase over time)
> SELECT COUNT(*) FROM beverage_metrics;

# View sample data
> SELECT * FROM beverage_metrics LIMIT 5;

# Check continuous aggregate is populated
> SELECT COUNT(*) FROM hourly_kpis;
```

### 6. Test Grafana Dashboards

```bash
# Access short-term dashboard
http://localhost:3000/d/ad74kv2

# Expected: 
# - Tank Level, Machine Status, Active Alarms show current values (Redis)
# - Line Speed, CO2 Pressure, Cycle Time show time-series graphs (TimescaleDB)

# For long-term dashboard (if needed)
docker exec -it timescaledb psql -U admin -d beverage_data \
  -c "CALL refresh_continuous_aggregate('hourly_kpis', NULL, NULL);"

# Then view
http://localhost:3000/d/adpzzbp
```

### End-to-End Test

```bash
# 1. Start fresh
docker-compose down -v
docker-compose up -d

# 2. Wait 30 seconds for services to initialize
sleep 30

# 3. Check OPC server is running
docker logs opc-server | grep "started"

# 4. Check bridge is connected
docker logs opc-mqtt-bridge | grep "Connected"

# 5. Check Redis has data
docker exec -it redis redis-cli HGETALL filling-line-1 | head -20

# 6. Check Kafka has messages
docker exec -it redpanda rpk topic consume beverage_metrics -n 1

# 7. Check database has data
docker exec -it timescaledb psql -U admin -d beverage_data \
  -c "SELECT COUNT(*) FROM beverage_metrics;"

# 8. View in Grafana
# Open http://localhost:3000
# Should see data flowing in real-time
```

---

## Service Ports & Access

| Service | Port | URL/Connection | Credentials |
|---------|------|---|---|
| **OPC-UA Server** | 4840 | opc.tcp://localhost:4840 | - |
| **MQTT Broker** | 1883 | mqtt://localhost:1883 | Anonymous |
| **Redis** | 6379 | redis://localhost:6379 | - |
| **Redpanda (Kafka)** | 19092 | localhost:19092 | - |
| **Redpanda Console** | 8080 | http://localhost:8080 | - |
| **TimescaleDB** | 5432 | localhost:5432 | admin / admin123 |
| **pgAdmin** | 5050 | http://localhost:5050 | admin@admin.com / admin123 |
| **Grafana** | 3000 | http://localhost:3000 | admin / admin123 |

---

**Author:** Tobias Gehrer  
**Course:** Advanced Data Management
**GitHub Repository:** https://github.com/TobiasGehrer/BeverageFillingLine