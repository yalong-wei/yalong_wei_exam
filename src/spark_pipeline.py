#!/usr/bin/env python3
"""
Spark Structured Streaming pipeline for AeroSense IoT sensor data.
Consumes from Kafka, processes, detects anomalies, computes windowed
aggregates, and writes to the three-zone data lake.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, from_unixtime, window, when, count, avg, min, max, sum,
    year as yr, month as mn, dayofmonth as dom, hour as hr, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, LongType
)

KAFKA_BOOTSTRAP = "localhost:9091,localhost:9092,localhost:9093"
TOPIC = "sensor-events"
CHECKPOINT_DIR = "/tmp/checkpoints"
DATALAKE_ROOT = "/tmp/datalake"

SENSOR_SCHEMA = StructType([
    StructField("sensor", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("source", StringType(), True),
    StructField("anomaly", BooleanType(), True),
])


def main():
    spark = SparkSession.builder \
        .appName("AeroSenseStreamingPipeline") \
        .master("local[1]") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON with explicit schema
    parsed_df = raw_df \
        .select(from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", from_unixtime(col("timestamp") / 1000).cast("timestamp")) \
        .dropna(subset=["sensor", "value", "timestamp"])

    # Filter out physically impossible values
    validated_df = parsed_df.filter(
        ((col("sensor") == "temperature") & (col("value").between(15.0, 45.0))) |
        ((col("sensor") == "humidity") & (col("value").between(30.0, 95.0))) |
        ((col("sensor") == "pressure") & (col("value").between(980.0, 1040.0)))
    )

    # Anomaly detection using business rules (independent of producer flag)
    anomaly_df = validated_df.withColumn(
        "is_anomaly",
        when((col("sensor") == "temperature") & (col("value") > 35.0), True)
        .when((col("sensor") == "humidity") & (col("value") > 90.0), True)
        .when((col("sensor") == "pressure") & ((col("value") < 990.0) | (col("value") > 1030.0)), True)
        .otherwise(False)
    )

    # --- 1. Raw zone: JSON partitioned by ingestion time ---
    raw_with_ingestion = anomaly_df \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumn("year", yr(col("ingestion_time")).cast("string")) \
        .withColumn("month", mn(col("ingestion_time")).cast("string")) \
        .withColumn("day", dom(col("ingestion_time")).cast("string")) \
        .withColumn("hour", hr(col("ingestion_time")).cast("string"))

    raw_query = raw_with_ingestion.writeStream \
        .format("json") \
        .option("path", os.path.join(DATALAKE_ROOT, "raw", "source=kafka", "topic=sensor-events")) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "raw")) \
        .partitionBy("year", "month", "day", "hour") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    # --- 2. Curated zone: Parquet partitioned by sensor_type / event_time ---
    # Rename 'sensor' to 'sensor_type' for Hive-style partition naming convention
    curated_df = anomaly_df \
        .withColumn("sensor_type", col("sensor")) \
        .withColumn("year", yr(col("event_time")).cast("string")) \
        .withColumn("month", mn(col("event_time")).cast("string")) \
        .withColumn("day", dom(col("event_time")).cast("string"))

    curated_query = curated_df.select(
        "sensor", "sensor_type", "value", "unit", "timestamp", "source",
        "anomaly", "is_anomaly", "event_time", "year", "month", "day"
    ).writeStream \
        .format("parquet") \
        .option("path", os.path.join(DATALAKE_ROOT, "curated", "domain=iot")) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "curated")) \
        .option("compression", "snappy") \
        .partitionBy("sensor_type", "year", "month", "day") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    # --- 3. Consumption zone: windowed aggregates ---
    windowed_df = anomaly_df \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("sensor")
        ) \
        .agg(
            avg("value").alias("mean_value"),
            min("value").alias("min_value"),
            max("value").alias("max_value"),
            count("*").alias("observation_count"),
            sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count")
        ) \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window") \
        .withColumn("sensor_type", col("sensor")) \
        .withColumn("year", yr(col("window_start")).cast("string")) \
        .withColumn("month", mn(col("window_start")).cast("string"))

    consumption_query = windowed_df.writeStream \
        .format("parquet") \
        .option("path", os.path.join(DATALAKE_ROOT, "consumption", "use_case=sensor_averages")) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "consumption")) \
        .option("compression", "snappy") \
        .partitionBy("sensor_type", "year", "month") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
