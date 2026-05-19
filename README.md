# AeroSense IoT Data Engineering Platform

## 1. Overview

AeroSense is an end-to-end data engineering platform for industrial environmental monitoring. The platform ingests IoT sensor readings (temperature, humidity, pressure) through a 3-broker Kafka cluster, processes them in real-time using Spark Structured Streaming, stores the results in a three-zone data lake (raw / curated / consumption), and exposes them through a REST API with six endpoints.

This project was built as part of the XICS404 Big Data Engineering final exam at EFREI Paris.

**Technologies:**
- Apache Kafka 7.5 (Confluent) — 3-broker KRaft cluster, RF=3, min.insync.replicas=2
- PySpark 3.5.3 — Structured Streaming, Spark SQL, watermarking, windowed aggregation
- Flask 3.0 — REST API with input validation and consistent JSON error handling
- Parquet (Snappy compression) — Columnar storage for curated and consumption zones

## 2. Architecture

```
+-------------------------+
|   Python Producer       |
|   (producer.py)        |
+-----------+-------------+
            |
            v
+-------------------------+
|  Kafka Cluster (3 br.)  |
|  topic: sensor-events   |
|  partitions: 3, RF: 3   |
+-----------+-------------+
            |
    +-------+--------+
    |                |
    v                v
+-----------------------+   +-----------------------+
| Spark Structured Str. |   |   REST API (Flask)    |
| - parse JSON          |   |   GET  /health        |
| - validate ranges     |   |   GET  /sensors       |
| - anomaly detection   |   |   GET  /latest        |
| - 5-min windowed agg  |   |   GET  /stats         |
| - 3-zone Parquet sink  |   |   GET  /anomalies     |
+-----------+-----------+   |   POST /readings      |
            |               +----------+------------+
            v                          |
+---------------------------+          |
|  Data Lake (/tmp/datalake)|<---------+
|  raw/   (JSON, ingestion) |
|  curated/ (Parquet, event) |
|  consumption/ (aggregates) |
+---------------------------+
```

## 3. Instructions

### Prerequisites
- Docker / Docker Compose (20.10+ / v2.0+)
- Python 3.9+ with Java 17+
- Apache Spark 3.5.1

### Step-by-step execution

**1. Start the Kafka cluster:**
```bash
docker compose up -d
docker compose ps  # verify 4 containers running
```

**2. Create the topic:**
```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29091 \
  --create --topic sensor-events --partitions 3 --replication-factor 3

docker exec kafka1 kafka-topics --bootstrap-server kafka1:29091 \
  --describe --topic sensor-events
```

Expected output:
```
Topic: sensor-events  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
  Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
```

**3. Install dependencies:**
```bash
pip install -r requirements.txt
```

**4. Run the producer:**
```bash
python src/producer.py --count 500 --rate 20 --source site-A-rack-01
```

**5. Start the Spark streaming pipeline:**
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  src/spark_pipeline.py
```

**6. Run analytical queries (in a separate terminal, after the pipeline has processed data):**
```bash
spark-submit src/analytics.py
```

**7. Start the REST API:**
```bash
python src/api/app.py
```

**8. Test the API:**
```bash
curl -s http://localhost:5000/api/v1/health | python3 -m json.tool
bash tests/test_curl_commands.sh
```

## 4. Technical Choices

### Partitioning strategy for the curated zone
We chose `sensor_type / year / month / day` Hive-style partitioning. This aligns with the two most common query patterns: filtering by sensor type and filtering by date range. With three sensor types and daily granularity, partition pruning can skip roughly two-thirds of the data for a single-sensor query. A flat layout was considered but rejected because it forces full table scans even for targeted queries. The tradeoff is more small files, but Spark handles this well with file compaction options.

### Spark Structured Streaming outputMode
We use `append` mode for all three sinks. The raw and curated zones are append-only logs, so append mode is the only correct choice — `update` would duplicate records and `complete` would rewrite the entire output. For the consumption zone, even though we compute aggregates, we use append mode with a watermark so that only finalised window results are emitted. This means each window result is written exactly once after the 2-minute watermark passes.

### Replication factor and min.insync.replicas
We set `replication-factor=3` with `min.insync.replicas=2`. This configuration can tolerate one broker failure without data loss — if one broker goes down, two replicas remain and the ISR still meets the minimum requirement. We considered RF=2 but decided that it cannot survive a single failure while maintaining write availability, since the sole remaining replica would be the only copy. RF=3 with min ISR=2 is the standard recommendation for production-grade Kafka deployments.

### Event_time vs ingestion_time across zones
The raw zone uses ingestion time (when the record was processed by Spark) for partitioning, while the curated and consumption zones use event time (the sensor's timestamp). This separation is deliberate: the raw zone serves as a forensic audit trail showing what was received and when, while the analytical zones need to reflect the physical reality of when measurements were taken. The 2-minute watermark handles late-arriving data that would otherwise land in a wrong event-time partition.

### End-to-end delivery semantics
The platform provides at-least-once semantics. The producer uses `acks=all` with `retries=5`, ensuring messages are confirmed by all in-sync replicas before acknowledgment. Spark Structured Streaming checkpoints Kafka offsets per sink, so on restart it resumes from the last committed offset. The Parquet sinks are naturally idempotent for append mode — duplicate micro-batches create duplicate files but do not corrupt existing ones. True exactly-once would require distributed transactions across Kafka and the file system, which is overly complex for this use case. The consumption zone's windowed aggregates effectively deduplicate through aggregation.

## 5. Results

### Analytical Query Results (from 1024 records, 305 anomalies)

**Q1 — Top anomaly days:**
| year | month | day | total_readings | anomaly_count | anomaly_pct |
|------|-------|-----|----------------|---------------|-------------|
| 2026 | 5     | 19  | 1024           | 305           | 29.79       |

**Q2 — Per-sensor global statistics:**
| sensor      | global_mean | global_min | global_max | global_stddev | anomaly_rate_pct |
|-------------|-------------|------------|------------|---------------|-------------------|
| humidity    | 64.96       | 30.21      | 94.93      | 19.95         | 12.97             |
| pressure    | 1010.68     | 980.03     | 1039.55    | 17.90         | 37.38             |
| temperature | 31.03       | 15.06      | 44.93      | 8.85          | 39.33             |

**Q3 — Daily temperature evolution:**
| year | month | day | daily_mean_temp | daily_min_temp | daily_max_temp | daily_anomaly_count |
|------|-------|-----|-----------------|----------------|----------------|----------------------|
| 2026 | 5     | 19  | 31.03           | 15.06          | 44.93          | 140                  |

**Q4 — Partition pruning speedup:** Full scan 0.168s vs partition-pruned 0.097s → **1.74x speedup** on a small dataset. The speedup factor increases with larger datasets and more partition columns.

**Sample API responses:**
```json
// GET /api/v1/health
{"status": "ok", "timestamp": 1779153041820, "service": "aerosense-api"}

// GET /api/v1/sensors
{"sensors": [{"type": "temperature", "unit": "C"}, ...], "count": 3}

// POST /api/v1/readings
{"status": "published", "topic": "sensor-events", "partition": 2, "offset": 2390}
```

See `outputs/screenshots/` for Kafka UI screenshots and `outputs/analytics/` for full CSV exports.

## 6. Limitations and Improvements

1. **Single-node Spark**: The pipeline runs in local mode. With two extra days, we would deploy a standalone Spark cluster with multiple executors for parallel processing of Kafka partitions.

2. **No alerting**: The platform detects anomalies but does not push alerts. We would add a notification layer (webhooks, email) triggered by the consumption zone's anomaly counts exceeding a threshold.

3. **No schema registry**: Message validation relies on a hardcoded PySpark schema. Confluent Schema Registry would enforce schema evolution and prevent breaking changes.

4. **No orchestration**: Components are started manually in separate terminals. We would add a Docker Compose service for the Spark job and use Airflow for scheduling the analytics pass.

5. **Limited partition pruning data**: The current dataset is too small to show dramatic speedup differences. With more historical data, the speedup factor would be significantly higher.
