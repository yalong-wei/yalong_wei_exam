# Analytical Queries Results

This document explains the four Spark SQL queries executed by `analytics.py` and their actual results from our test run.

## Query 1: Top 5 Days with Highest Anomaly Count

This query groups all readings by year/month/day, counts total readings and anomaly flags, and sorts by anomaly count descending. It reveals which time windows had the most problematic sensor behaviour.

**Actual result:**
| year | month | day | total_readings | anomaly_count | anomaly_pct |
|------|-------|-----|----------------|---------------|-------------|
| 2026 | 5     | 19  | 1024           | 305           | 29.79       |

Since our test ran on a single day, there is only one row. With a multi-day dataset, this query would show temporal patterns in sensor anomalies.

## Query 2: Per-Sensor Global Statistics

Computes mean, min, max, standard deviation, total observations, and anomaly rate for each sensor type.

**Actual result:**
| sensor      | global_mean | global_min | global_max | global_stddev | total_observations | anomaly_rate_pct |
|-------------|-------------|------------|------------|---------------|---------------------|-------------------|
| humidity    | 64.96       | 30.21      | 94.93      | 19.95         | 347                 | 12.97             |
| pressure    | 1010.68     | 980.03     | 1039.55    | 17.90         | 321                 | 37.38             |
| temperature | 31.03       | 15.06      | 44.93      | 8.85          | 356                 | 39.33             |

The temperature sensor has the highest anomaly rate (39.33%) because the anomaly threshold is temperature > 35°C, which is close to the upper bound of the normal range (45°C). Pressure anomalies at 37.38% reflect the tight range of 990-1030 hPa within the overall 980-1040 hPa range. Humidity anomalies are the lowest at 12.97% since only values above 90% are flagged.

## Query 3: Daily Temperature Evolution

Groups temperature readings by day and computes daily mean, min, max, and anomaly count.

**Actual result:**
| year | month | day | daily_mean_temp | daily_min_temp | daily_max_temp | daily_anomaly_count |
|------|-------|-----|-----------------|----------------|----------------|----------------------|
| 2026 | 5     | 19  | 31.03           | 15.06          | 44.93          | 140                  |

With more days of data, this query would reveal trends such as a rising daily mean indicating HVAC issues.

## Query 4: Partition Pruning Demonstration

Compares execution times of two count operations:

1. **Full scan**: `SELECT COUNT(*) FROM sensor_readings` — reads all partitions.
2. **Partition-pruned**: `SELECT COUNT(*) FROM sensor_readings WHERE sensor_type = 'temperature' AND year = '2026'` — reads only the `sensor_type=temperature/year=2026` subdirectory.

**Actual result:**
| query_type        | count | execution_time_s | speedup_x |
|-------------------|-------|------------------|-----------|
| full_scan         | 1024  | 0.168            | 1.0       |
| partition_pruned  | 356   | 0.097            | 1.74      |

The speedup of 1.74x on a small dataset is modest because Spark's file listing overhead dominates when there are few files. With more data (hundreds of partitions, millions of rows), the speedup factor typically reaches 3-10x since Spark can skip entire directories without opening any Parquet files. This is the key benefit of Hive-style partitioning: predicate pushdown at the file system level.
