#!/usr/bin/env python3
"""
Analytical queries on the AeroSense data lake using Spark SQL.
Includes a partition pruning demonstration.
"""

import os
import time
from pyspark.sql import SparkSession

DATALAKE_ROOT = "/tmp/datalake"
OUTPUT_DIR = "outputs/analytics"


def run_queries(spark):
    # Register the curated data as a temp view
    curated_path = os.path.join(DATALAKE_ROOT, "curated", "domain=iot")
    df = spark.read.parquet(curated_path)
    df.createOrReplaceTempView("sensor_readings")

    # Register the consumption zone
    consumption_path = os.path.join(DATALAKE_ROOT, "consumption", "use_case=sensor_averages")
    try:
        agg_df = spark.read.parquet(consumption_path)
        agg_df.createOrReplaceTempView("sensor_averages")
    except Exception:
        print("Warning: consumption zone not found, skipping aggregation queries")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    results = {}

    # ---------- Query 1: Top 5 days with the most anomalies ----------
    print("\n=== Query 1: Top 5 days with highest anomaly count ===")
    q1 = spark.sql("""
        SELECT
            year, month, day,
            COUNT(*) as total_readings,
            SUM(CASE WHEN is_anomaly = true THEN 1 ELSE 0 END) as anomaly_count,
            ROUND(100.0 * SUM(CASE WHEN is_anomaly = true THEN 1 ELSE 0 END) / COUNT(*), 2) as anomaly_pct
        FROM sensor_readings
        GROUP BY year, month, day
        ORDER BY anomaly_count DESC
        LIMIT 5
    """)
    q1.show(truncate=False)
    q1_df = q1.toPandas()
    q1_df.to_csv(os.path.join(OUTPUT_DIR, "q1_top_anomaly_hours.csv"), index=False)
    results["q1"] = q1_df

    # ---------- Query 2: Per-sensor statistics ----------
    print("\n=== Query 2: Per-sensor global statistics ===")
    q2 = spark.sql("""
        SELECT
            sensor,
            ROUND(MEAN(value), 2) as global_mean,
            ROUND(MIN(value), 2) as global_min,
            ROUND(MAX(value), 2) as global_max,
            ROUND(STDDEV(value), 2) as global_stddev,
            COUNT(*) as total_observations,
            ROUND(100.0 * SUM(CASE WHEN is_anomaly = true THEN 1 ELSE 0 END) / COUNT(*), 2) as anomaly_rate_pct
        FROM sensor_readings
        GROUP BY sensor
        ORDER BY sensor
    """)
    q2.show(truncate=False)
    q2_df = q2.toPandas()
    q2_df.to_csv(os.path.join(OUTPUT_DIR, "q2_sensor_stats.csv"), index=False)
    results["q2"] = q2_df

    # ---------- Query 3: Daily temperature evolution ----------
    print("\n=== Query 3: Daily temperature evolution ===")
    q3 = spark.sql("""
        SELECT
            year, month, day,
            ROUND(AVG(value), 2) as daily_mean_temp,
            ROUND(MIN(value), 2) as daily_min_temp,
            ROUND(MAX(value), 2) as daily_max_temp,
            SUM(CASE WHEN is_anomaly = true THEN 1 ELSE 0 END) as daily_anomaly_count
        FROM sensor_readings
        WHERE sensor = 'temperature'
        GROUP BY year, month, day
        ORDER BY year, month, day
    """)
    q3.show(truncate=False)
    q3_df = q3.toPandas()
    q3_df.to_csv(os.path.join(OUTPUT_DIR, "q3_daily_temperature.csv"), index=False)
    results["q3"] = q3_df

    # ---------- Query 4: Partition pruning demonstration ----------
    print("\n=== Query 4: Partition pruning demonstration ===")
    print("Running count WITHOUT partition filter...")
    t1_start = time.time()
    full_count = spark.sql("SELECT COUNT(*) as cnt FROM sensor_readings").collect()[0][0]
    t1_end = time.time()
    t1 = t1_end - t1_start
    print(f"  Full scan count: {full_count}, time: {t1:.3f}s")

    print("Running count WITH partition filter (sensor_type='temperature', year='2026')...")
    t2_start = time.time()
    filtered_count = spark.sql("""
        SELECT COUNT(*) as cnt
        FROM sensor_readings
        WHERE sensor_type = 'temperature' AND year = '2026'
    """).collect()[0][0]
    t2_end = time.time()
    t2 = t2_end - t2_start
    print(f"  Filtered scan count: {filtered_count}, time: {t2:.3f}s")

    speedup = round(t1 / t2, 2) if t2 > 0 else float('inf')
    print(f"  Speedup factor: {speedup}x")

    pr_df = q1_df.__class__({
        "query_type": ["full_scan", "partition_pruned"],
        "count": [int(full_count), int(filtered_count)],
        "execution_time_s": [round(t1, 3), round(t2, 3)],
        "speedup_x": [1.0, speedup],
    })
    pr_df.to_csv(os.path.join(OUTPUT_DIR, "q4_partition_pruning.csv"), index=False)
    results["q4"] = pr_df

    print(f"\nAll results saved to {OUTPUT_DIR}/")
    return results


def main():
    spark = SparkSession.builder \
        .appName("AeroSenseAnalytics") \
        .master("local[1]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    run_queries(spark)
    spark.stop()


if __name__ == "__main__":
    main()
