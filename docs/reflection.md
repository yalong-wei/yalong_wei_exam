# Reflection Questions

## Question 1: Pipeline Crash During Processing

**Your pipeline crashes during processing, after writing to the raw zone but before writing to the curated zone. What is the impact on the data? Which checkpoint strategy prevents this issue?**

If the pipeline crashes between the raw and curated writes, the data lake ends up in an inconsistent state: the raw zone has records from the latest micro-batch, but the curated and consumption zones are missing those same records. On restart, without proper checkpointing, the Kafka offsets would already have been committed past those records, so the pipeline would skip them — the raw zone data becomes orphaned, present but never propagated downstream.

The way to avoid this is exactly what we implemented: separate checkpoint directories per sink (`/tmp/checkpoints/raw`, `/tmp/checkpoints/curated`, `/tmp/checkpoints/consumption`). Spark Structured Streaming tracks which offsets have been committed by each sink independently. When the pipeline restarts after the crash, Spark checks the curated checkpoint, sees it is behind the raw checkpoint, and re-processes the micro-batch starting from the last offset the curated sink successfully committed. The raw sink, having already written those records, will skip the duplicate write because its checkpoint indicates those offsets are done. This gives us per-sink exactly-once semantics: each sink writes each micro-batch exactly once, even across restarts.

The key insight is that a single shared checkpoint for all three sinks would not solve this problem — we need independent tracking because the three writes are not atomic. The per-sink checkpoint approach is the practical solution; a true atomic write across three file system paths would require two-phase commit, which is overkill for a data lake.

## Question 2: Scaling to 50,000 Messages Per Second

**You scale the producer up to 50,000 messages per second. In your opinion, what would be the first bottlenecks in your current architecture, and how would you fix them?**

The first bottleneck would be the Python producer itself. Our current implementation sends messages one at a time with `producer.send()` and a sleep between events. At 50K msg/s, Python's GIL and the overhead of JSON serialisation in a single thread would cap throughput at maybe 5K-10K msg/s. To fix this, I would increase `linger_ms` to 20-50ms and `batch_size` to 65536 to let the Kafka client batch messages internally, and also run 5-10 producer instances in parallel, each handling a different source identifier.

The second bottleneck is the Spark pipeline running in local mode with a single thread. A single executor cannot keep up with 50K msg/s from three Kafka partitions. The fix is to switch from `local[1]` to a proper Spark cluster with at least 3 executors (one per partition), and increase `spark.executor.memory` to 2-4 GB. I would also bump the trigger interval from 10 seconds to 30 seconds so each micro-batch is larger, which reduces the per-batch overhead of checkpoint writes and file listing.

The third bottleneck is Kafka's partition count. Three partitions means only three consumers can read in parallel. At 50K msg/s, each partition would handle ~17K msg/s, which is feasible but leaves no headroom. I would increase the partition count to 9 or 12 and add more Spark executors accordingly.

## Question 3: Kafka vs Parquet Data Lake as Source of Truth

**Compare the advantages and drawbacks of using Kafka as the source of truth for historical data, versus a Parquet data lake. In which scenarios should each be preferred?**

Kafka is excellent for real-time access: consumers can subscribe to a topic and receive new records within milliseconds. It also preserves ordering within a partition and provides strong durability guarantees through replication. However, Kafka is not designed for historical queries. To find all temperature readings above 40°C last month, you would need to consume the entire topic from the beginning, which is impractical. Kafka also has a retention period — once data expires, it is gone unless you explicitly extend retention, which gets expensive at scale.

Parquet, on the other hand, is built for analytical queries. Columnar storage means you only read the columns you need, and partition pruning lets you skip entire directories. A query filtering on `sensor_type='temperature'` and `year='2026'` reads only the relevant subdirectory instead of scanning everything. Parquet files also have no retention limit — they sit on disk until you delete them, and Snappy compression keeps storage costs low.

The tradeoff is that Parquet is not real-time. There is inherent latency from the Spark micro-batch interval (10 seconds in our case) plus file listing and opening overhead. You cannot subscribe to a Parquet file and get notified of new rows.

In practice, I would use Kafka as the source of truth for the last 24-72 hours of operational data (dashboards, alerting, real-time decisions), and Parquet as the source of truth for anything older (historical reports, ML training, compliance audits). The Spark pipeline bridges the two by reading from Kafka and writing to Parquet, so both are always in sync.

## Question 4: Sensor Emitting Aberrant Values for 2 Hours

**A sensor breaks and emits aberrant values for 2 hours. How does your architecture detect this case? How would you isolate these data points without deleting them?**

Our pipeline detects this in two ways. First, the Spark anomaly detection step computes an `is_anomaly` column for every record using business rules (temperature > 35°C, humidity > 90%, pressure outside 990-1030 hPa). A broken sensor would trigger this flag on nearly every reading. Second, the consumption zone aggregates anomaly counts per 5-minute window, so a spike from 0-2 anomalies per window to 20+ would be immediately visible. Query 1 from `analytics.py` (top anomaly hours) would flag those 2 hours as the worst offenders.

To isolate the bad data without deleting it, I would use the `source` field in the curated zone. Each record carries a `source` identifier like `site-A-rack-12`. If the broken sensor is at `site-B-rack-07`, all downstream queries can add `WHERE source != 'site-B-rack-07'` to exclude it. The `is_anomaly` flag also serves as a filter — reports can default to `WHERE is_anomaly = false` and only include anomalous readings when explicitly requested.

For the API, I would add a `filter_anomalies=true` query parameter to the stats endpoint that excludes flagged readings by default. The raw zone should never be modified — it is the immutable audit trail. The principle is to flag and filter, not delete.

## Question 5: Adding a New Sensor Type (CO2)

**You must add a new sensor type co2. Which parts of your pipeline must be modified? Give a precise list of files and changes.**

1. **`src/producer.py`**: Add `"co2": {"unit": "ppm", "min": 400.0, "max": 2000.0, "anomaly_threshold": 1500.0}` to the `SENSOR_CONFIG` dictionary. In `build_message()`, add a branch for co2 anomaly generation — values above 1500 ppm would be flagged as anomalies.

2. **`src/spark_pipeline.py`**: Three changes. First, add a validation condition in `validated_df`: `((col("sensor") == "co2") & (col("value").between(400.0, 2000.0)))`. Second, add an anomaly rule in the `is_anomaly` column: `.when((col("sensor") == "co2") & (col("value") > 1500.0), True)`. No changes to the data lake writes or partitioning — since `sensor_type` is already a partition column, CO2 data will automatically create `sensor_type=co2/` subdirectories in both curated and consumption zones.

3. **`src/api/app.py`**: Add `"co2": {"unit": "ppm"}` to the `SENSOR_MAP` dictionary so the API recognises it as a valid sensor type.

4. **`src/api/lake_utils.py`**: Add `"co2"` to the `ALLOWED_SENSORS` list.

5. **`docs/reflection.md`**: Not a code change, but worth noting that the reflection answers would need updating if the anomaly thresholds change.

Files that do NOT need changes: `docker-compose.yml`, `requirements.txt`, `src/analytics.py` (queries group by sensor and will automatically include co2), `src/consumer.py`, `src/api/kafka_utils.py`, and the test script (it already tests with arbitrary sensor types).
