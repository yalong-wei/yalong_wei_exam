"""
Kafka utility functions for the AeroSense REST API.
"""
from kafka import KafkaProducer, KafkaConsumer
import json
import time

BOOTSTRAP_SERVERS = "localhost:9091,localhost:9092,localhost:9093"
TOPIC = "sensor-events"


def get_last_message(sensor_type, timeout=5.0):
    """
    Read the most recent message for a given sensor type from Kafka.
    Subscribes and reads the latest offset only.
    """
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=int(timeout * 1000),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )

    last_msg = None
    start = time.time()
    for msg in consumer:
        if msg.key == sensor_type:
            last_msg = msg.value
        if time.time() - start > timeout:
            break

    consumer.close()
    return last_msg


def get_anomalies(sensor_type=None, limit=20, timeout=3.0):
    """
    Read recent messages from Kafka and filter anomalies.
    """
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=int(timeout * 1000),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )

    anomalies = []
    start = time.time()
    for msg in consumer:
        val = msg.value
        if val is None:
            continue
        if sensor_type and msg.key != sensor_type:
            continue
        if val.get("anomaly", False):
            anomalies.append(val)
            if len(anomalies) >= limit:
                break
        if time.time() - start > timeout:
            break

    consumer.close()
    return anomalies


def send_reading(message):
    """
    Publish a sensor reading to Kafka.
    """
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",
        retries=3,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    key = message["sensor"]
    future = producer.send(TOPIC, key=key, value=message)
    producer.flush()
    meta = future.get(timeout=5)
    return meta
