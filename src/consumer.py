#!/usr/bin/env python3
"""
Simple Kafka consumer for testing purposes.
Reads messages from the sensor-events topic and prints them.
"""

import argparse
import json
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9091,localhost:9092,localhost:9093"
TOPIC = "sensor-events"


def main():
    parser = argparse.ArgumentParser(description="AeroSense test consumer")
    parser.add_argument("--max", type=int, default=10, help="Max messages to read")
    parser.add_argument("--sensor", type=str, default=None, help="Filter by sensor type")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=10000,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )

    count = 0
    for msg in consumer:
        if args.sensor and msg.key != args.sensor:
            continue
        print(f"[{msg.key}] {json.dumps(msg.value, indent=2)}")
        count += 1
        if count >= args.max:
            break

    consumer.close()
    print(f"\nRead {count} messages")


if __name__ == "__main__":
    main()
