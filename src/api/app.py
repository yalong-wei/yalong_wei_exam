#!/usr/bin/env python3
"""
AeroSense REST API - Flask application.
Exposes the IoT sensor platform data through RESTful endpoints.
"""

import json
import time
import sys
import os
from flask import Flask, request, jsonify

# Allow running as script directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kafka_utils import get_last_message, get_anomalies, send_reading
from lake_utils import get_sensor_types, get_latest_reading, get_daily_stats

app = Flask(__name__)

SENSOR_MAP = {
    "temperature": {"unit": "C"},
    "humidity": {"unit": "%"},
    "pressure": {"unit": "hPa"},
}


@app.route("/api/v1/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "ok",
        "timestamp": int(time.time() * 1000),
        "service": "aerosense-api",
    }), 200


@app.route("/api/v1/sensors", methods=["GET"])
def list_sensors():
    """List all available sensor types."""
    types = get_sensor_types()
    return jsonify({
        "sensors": [
            {"type": t, "unit": SENSOR_MAP[t]["unit"]}
            for t in types
        ],
        "count": len(types),
    }), 200


@app.route("/api/v1/sensors/<sensor_type>/latest", methods=["GET"])
def latest_reading(sensor_type):
    """Get the latest reading for a given sensor type from Kafka."""
    if sensor_type not in SENSOR_MAP:
        return jsonify({"error": f"Unknown sensor type: {sensor_type}", "valid_types": list(SENSOR_MAP.keys())}), 404

    msg = get_last_message(sensor_type, timeout=5.0)
    if msg is None:
        return jsonify({"error": f"No recent reading found for {sensor_type}"}), 404

    return jsonify({
        "sensor": msg["sensor"],
        "value": msg["value"],
        "unit": msg["unit"],
        "timestamp": msg["timestamp"],
        "source": msg["source"],
        "anomaly": msg["anomaly"],
    }), 200


@app.route("/api/v1/sensors/<sensor_type>/stats", methods=["GET"])
def sensor_stats(sensor_type):
    """Get daily aggregated statistics from the data lake."""
    if sensor_type not in SENSOR_MAP:
        return jsonify({"error": f"Unknown sensor type: {sensor_type}", "valid_types": list(SENSOR_MAP.keys())}), 404

    days_str = request.args.get("days", "7")
    try:
        days = int(days_str)
    except ValueError:
        return jsonify({"error": "days must be an integer"}), 400

    if days < 1 or days > 90:
        return jsonify({"error": "days must be between 1 and 90"}), 400

    stats = get_daily_stats(sensor_type, days=days)
    if not stats:
        return jsonify({"error": f"No statistics available for {sensor_type}"}), 404

    return jsonify({
        "sensor": sensor_type,
        "days": days,
        "stats": stats,
    }), 200


@app.route("/api/v1/anomalies", methods=["GET"])
def list_anomalies():
    """List recent anomaly readings."""
    sensor = request.args.get("sensor", None)
    limit_str = request.args.get("limit", "20")

    if sensor and sensor not in SENSOR_MAP:
        return jsonify({"error": f"Unknown sensor type: {sensor}", "valid_types": list(SENSOR_MAP.keys())}), 400

    try:
        limit = int(limit_str)
    except ValueError:
        return jsonify({"error": "limit must be an integer"}), 400

    if limit < 1 or limit > 100:
        return jsonify({"error": "limit must be between 1 and 100"}), 400

    anomalies = get_anomalies(sensor_type=sensor, limit=limit)
    return jsonify({
        "anomalies": anomalies,
        "count": len(anomalies),
    }), 200


@app.route("/api/v1/readings", methods=["POST"])
def publish_reading():
    """Publish a sensor reading to Kafka."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Request body must be valid JSON"}), 400

    # Validate required fields
    sensor = data.get("sensor")
    value = data.get("value")

    if not sensor:
        return jsonify({"error": "Missing required field: sensor"}), 400
    if sensor not in SENSOR_MAP:
        return jsonify({"error": f"Invalid sensor type: {sensor}", "valid_types": list(SENSOR_MAP.keys())}), 422
    if value is None:
        return jsonify({"error": "Missing required field: value"}), 400

    try:
        value = float(value)
    except (TypeError, ValueError):
        return jsonify({"error": "value must be a number"}), 422

    # Build the message
    message = {
        "sensor": sensor,
        "value": value,
        "unit": SENSOR_MAP[sensor]["unit"],
        "timestamp": int(time.time() * 1000),
        "source": data.get("source", "api-ingest"),
        "anomaly": False,
    }

    try:
        meta = send_reading(message)
        return jsonify({
            "status": "published",
            "topic": meta.topic,
            "partition": meta.partition,
            "offset": meta.offset,
            "message": message,
        }), 201
    except Exception as e:
        app.logger.error(f"Failed to publish reading: {e}")
        return jsonify({"error": "Failed to publish reading to Kafka"}), 500


# Global error handlers
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Not found", "path": request.path}), 404


@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({"error": "Method not allowed", "method": request.method, "path": request.path}), 405


@app.errorhandler(500)
def server_error(e):
    app.logger.error(f"Internal server error: {e}")
    return jsonify({"error": "Internal server error"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
