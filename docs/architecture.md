# Architecture Overview

This document describes the detailed architecture of the AeroSense IoT data engineering platform.

## System Components

### 1. Data Generation Layer (producer.py)

The Python producer simulates IoT sensor readings for three sensor types (temperature, humidity, pressure). It generates values within physically plausible ranges and injects approximately 12% anomalies to enable testing of the anomaly detection pipeline. Messages are serialised as JSON and published to Kafka with the sensor type as the message key, ensuring partition-based ordering guarantees per sensor type.

Configuration: `acks=all`, `retries=5`, `max_in_flight_requests_per_connection=1`, `linger_ms=10`, `batch_size=16384`.

### 2. Message Broker Layer (Kafka)

A 3-broker Kafka cluster running in KRaft mode (no ZooKeeper dependency) serves as the ingestion backbone. The `sensor-events` topic is configured with 3 partitions and a replication factor of 3, providing fault tolerance and parallel consumption. Each partition is mapped to a consistent hash of the sensor type key, ensuring all readings for a given sensor type land in the same partition.

Kafka UI is accessible at `http://localhost:8081` for monitoring topics, consumer lag, and broker health.

### 3. Stream Processing Layer (spark_pipeline.py)

Spark Structured Streaming reads from Kafka with an explicit JSON schema. The pipeline performs four stages:

- **Parsing and validation**: JSON payloads are parsed with an explicit schema. Records with null fields or values outside physical ranges (temperature 15-45C, humidity 30-95%, pressure 980-1040 hPa) are dropped.
- **Anomaly detection**: Each record is independently evaluated against business-defined thresholds (temperature > 35C, humidity > 90%, pressure < 990 or > 1030 hPa). This is computed separately from the producer's self-declared anomaly flag.
- **Windowed aggregation**: A 5-minute tumbling window with a 2-minute watermark computes mean, min, max, observation count, and anomaly count per sensor type.
- **Multi-sink write**: The pipeline writes to three zones simultaneously using separate checkpoint directories.

### 4. Storage Layer (Data Lake)

The three-zone architecture follows data engineering best practices:

- **Raw zone** (`raw/source=kafka/topic=sensor-events/year=.../month=.../day=.../hour=...`): Original JSON payloads partitioned by ingestion time. Serves as a source-of-truth backup and enables debugging.
- **Curated zone** (`curated/domain=iot/sensor_type=.../year=.../month=.../day=...`): Cleaned, validated Parquet files with Snappy compression, partitioned by sensor type and event time. This is the primary zone for analytical queries.
- **Consumption zone** (`consumption/use_case=sensor_averages/sensor_type=.../year=.../month=...`): Pre-computed windowed aggregates in Parquet, partitioned by sensor type and month. Optimised for dashboard and API consumption.

### 5. API Layer (Flask)

The REST API provides six endpoints for programmatic access. It reads from both Kafka (for live data via the `/latest` endpoint) and the data lake (for historical statistics via the `/stats` endpoint). The API uses Spark for Parquet reads and the kafka-python-ng library for Kafka interactions.

Endpoints:
| Verb | URL | Description | Status Codes |
|------|-----|-------------|-------------|
| GET | /api/v1/health | Health check | 200 |
| GET | /api/v1/sensors | List sensor types | 200 |
| GET | /api/v1/sensors/\<type\>/latest | Latest Kafka reading | 200/404 |
| GET | /api/v1/sensors/\<type\>/stats?days=N | Daily stats from Parquet | 200/400/404 |
| GET | /api/v1/anomalies?sensor=\<type\>&limit=N | Recent anomalies | 200/400 |
| POST | /api/v1/readings | Publish reading to Kafka | 201/400/422 |

### 6. Analytics Layer (analytics.py)

A Spark SQL batch script runs four analytical queries on the curated data lake. It demonstrates partition pruning by comparing execution times with and without partition-column filters.

## Data Flow

```
Producer -> Kafka (sensor-events) -> Spark Stream -> Data Lake
                                                           |
                                     Flask API <-----------+
                                     Spark SQL Analytics <-+
```
