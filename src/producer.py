#!/usr/bin/env python3
"""
Producer script for AeroSense IoT sensor data.
Generates simulated sensor readings and publishes them to Kafka.
"""

import argparse
import json
import random
import time
import signal
import sys

from kafka import KafkaProducer

SENSOR_CONFIG = {
    "temperature": {"unit": "C",  "min": 15.0, "max": 45.0, "anomaly_threshold": 35.0},
    "humidity":    {"unit": "%",  "min": 30.0, "max": 95.0, "anomaly_threshold": 90.0},
    "pressure":    {"unit": "hPa","min": 980.0,"max": 1040.0,"anomaly_threshold_low": 990.0, "anomaly_threshold_high": 1030.0},
}

ANOMALY_RATIO = 0.12  # a bit more than 10% so we can actually test anomaly detection

BOOTSTRAP_SERVERS = "localhost:9091,localhost:9092,localhost:9093"
TOPIC = "sensor-events"

producer = None


def build_message(sensor_type, source):
    cfg = SENSOR_CONFIG[sensor_type]
    is_anomaly = random.random() < ANOMALY_RATIO

    if is_anomaly:
        # generate an out-of-range value
        if sensor_type == "temperature":
            value = random.uniform(35.1, 50.0)
        elif sensor_type == "humidity":
            value = random.uniform(90.1, 100.0)
        elif sensor_type == "pressure":
            if random.random() < 0.5:
                value = random.uniform(950.0, 989.9)
            else:
                value = random.uniform(1030.1, 1060.0)
    else:
        value = round(random.uniform(cfg["min"], cfg["max"]), 2)

    return {
        "sensor": sensor_type,
        "value": value,
        "unit": cfg["unit"],
        "timestamp": int(time.time() * 1000),
        "source": source,
        "anomaly": is_anomaly,
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}", file=sys.stderr)


def shutdown(sig, frame):
    print("\nFlushing and shutting down...")
    if producer:
        producer.flush()
    sys.exit(0)


def main():
    global producer

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    parser = argparse.ArgumentParser(description="AeroSense IoT sensor data producer")
    parser.add_argument("--count", type=int, default=100, help="Number of events to produce")
    parser.add_argument("--rate", type=int, default=10, help="Events per second")
    parser.add_argument("--source", type=str, default="site-A-rack-01", help="Source identifier")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=10,
        batch_size=16384,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sensor_types = list(SENSOR_CONFIG.keys())
    delay = 1.0 / args.rate if args.rate > 0 else 0.1

    print(f"Producing {args.count} events at {args.rate} eps from {args.source}")
    sent = 0
    anomaly_sent = 0

    for i in range(args.count):
        sensor_type = random.choice(sensor_types)
        msg = build_message(sensor_type, args.source)
        key = sensor_type

        producer.send(TOPIC, key=key, value=msg)
        sent += 1
        if msg["anomaly"]:
            anomaly_sent += 1

        if sent % 50 == 0:
            print(f"  Sent {sent}/{args.count} events (anomalies: {anomaly_sent})")

        time.sleep(delay)

    producer.flush()
    print(f"Done. Sent {sent} events, {anomaly_sent} anomalies ({100*anomaly_sent/sent:.1f}%)")


if __name__ == "__main__":
    main()
