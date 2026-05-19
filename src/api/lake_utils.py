"""
Data lake utility functions for the AeroSense REST API.
Reads Parquet files from the curated and consumption zones.
"""
import os
from pyspark.sql import SparkSession

DATALAKE_ROOT = "/tmp/datalake"
ALLOWED_SENSORS = ["temperature", "humidity", "pressure"]

# We init Spark once and reuse it
_spark = None


def _get_spark():
    global _spark
    if _spark is None:
        _spark = SparkSession.builder \
            .appName("AeroSenseAPI") \
            .master("local[1]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def get_sensor_types():
    """Return the list of known sensor types."""
    return ALLOWED_SENSORS


def get_latest_reading(sensor_type):
    """
    Read the most recent reading for a sensor from the curated zone.
    """
    spark = _get_spark()
    curated_path = os.path.join(DATALAKE_ROOT, "curated", "domain=iot")
    try:
        df = spark.read.parquet(curated_path) \
            .filter(f"sensor_type = '{sensor_type}'") \
            .orderBy("timestamp", ascending=False) \
            .limit(1)
        rows = df.collect()
        if rows:
            row = rows[0]
            return {
                "sensor": row["sensor"],
                "value": float(row["value"]),
                "unit": row["unit"],
                "timestamp": int(row["timestamp"]),
                "source": row["source"],
                "anomaly": bool(row["anomaly"]),
                "is_anomaly": bool(row["is_anomaly"]),
            }
    except Exception as e:
        print(f"Error reading curated data: {e}")
    return None


def get_daily_stats(sensor_type, days=7):
    """
    Read daily aggregated stats from the consumption zone.
    """
    spark = _get_spark()
    consumption_path = os.path.join(DATALAKE_ROOT, "consumption", "use_case=sensor_averages")
    try:
        df = spark.read.parquet(consumption_path) \
            .filter(f"sensor_type = '{sensor_type}'") \
            .orderBy("window_start", ascending=False) \
            .limit(days * 12)  # ~12 windows per day (5min windows)

        rows = df.collect()
        stats = []
        for row in rows:
            stats.append({
                "window_start": str(row["window_start"]),
                "window_end": str(row["window_end"]),
                "mean_value": float(row["mean_value"]) if row["mean_value"] else None,
                "min_value": float(row["min_value"]) if row["min_value"] else None,
                "max_value": float(row["max_value"]) if row["max_value"] else None,
                "observation_count": int(row["observation_count"]),
                "anomaly_count": int(row["anomaly_count"]),
            })
        return stats
    except Exception as e:
        print(f"Error reading consumption data: {e}")
        return []
