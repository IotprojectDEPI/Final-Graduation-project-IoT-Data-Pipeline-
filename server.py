# server.py - Flask API + Kafka Alerts + WebSocket Notifications

from flask import Flask, jsonify
from flask_cors import CORS
from flask_sock import Sock
import sqlite3
import pandas as pd
from datetime import datetime
import os
import threading
import json
from kafka import KafkaConsumer
import queue

# -----------------------------
# Flask Setup
# -----------------------------
app = Flask(__name__)
CORS(app)
sock = Sock(app)

DB_PATH = "Readings.db"

# A thread-safe queue to store live alerts
alert_queue = queue.Queue()

# Connected WebSocket clients
connected_clients = set()


# -----------------------------
# Fetch the latest 11 rows (same code you had)
# -----------------------------
def get_latest_readings():
    try:
        if not os.path.exists(DB_PATH):
            return []

        conn = sqlite3.connect(DB_PATH)
        query = "SELECT * FROM Readings ORDER BY Timestamp DESC LIMIT 11"
        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            return []

        df = df.iloc[::-1].reset_index(drop=True)

        fish_species = [
            "pangas", "rui", "silverCup", "koi", "tilapia",
            "shrimp", "sing", "prawn", "katla", "karpio", "magur"
        ]

        result = []
        for i, (_, row) in enumerate(df.iterrows()):
            fish = row.get("fish", fish_species[i]).strip()

            entry = {
                "fish": fish,
                "timestamp": row.get("Timestamp", ""),
                "temperature": float(row.get("temperature", 0)),
                "humidity": float(row.get("humidity", 0)),
                "light": float(row.get("light", 0)),
                "pH": float(row.get("pH", 0)),
                "electrical_conductivity": float(row.get("electrical_conductivity", 0)),
                "turbidity": float(row.get("turbidity", 0)),
                "water_temp": float(row.get("water_temp", 0)),
                "TDS": int(row.get("TDS", 0)),

                # classifications
                "temperature_class": row.get("temperature_class", "N/A"),
                "humidity_class": row.get("humidity_class", "N/A"),
                "light_class": row.get("light_class", "N/A"),
                "pH_class": row.get("pH_class", "N/A"),
                "electrical_conductivity_class": row.get("electrical_conductivity_class", "N/A"),
                "turbidity_class": row.get("turbidity_class", "N/A"),
                "water_temp_class": row.get("water_temp_class", "N/A"),
                "TDS_class": row.get("TDS_class", "N/A"),
            }

            result.append(entry)

        return result

    except Exception as e:
        print("ERROR in get_latest_readings:", e)
        return []


# -----------------------------
# API Endpoint
# -----------------------------
@app.route("/api/data")
def api_data():
    data = get_latest_readings()
    if not data:
        return jsonify({"success": False, "error": "No data"}), 404
    return jsonify({"success": True, "data": data})


# -----------------------------
# WebSocket for Alerts
# -----------------------------
@sock.route('/alerts')
def alerts_socket(ws):
    print("ðŸ”Œ Client connected to /alerts")
    connected_clients.add(ws)

    try:
        while True:
            msg = alert_queue.get()   # wait for new alert
            ws.send(msg)
    except:
        pass
    finally:
        connected_clients.remove(ws)
        print("âŒ Client disconnected")


# -----------------------------
# Kafka Consumer Thread
# -----------------------------
def kafka_alert_loop():
    print("ðŸ“¡ Starting Kafka Alert Listener...")

    consumer = KafkaConsumer(
        "iot-sensor-data",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: m.decode("utf-8")
    )

    for message in consumer:
        print(f"ðŸš¨ Incoming alert: {message.value}")

        # Add alert to queue
        alert_queue.put(message.value)

        # Push to all connected WebSocket clients
        dead_clients = []
        for client in connected_clients:
            try:
                client.send(message.value)
            except:
                dead_clients.append(client)

        for dc in dead_clients:
            connected_clients.remove(dc)


# Launch Kafka thread on startup
threading.Thread(target=kafka_alert_loop, daemon=True).start()


# -----------------------------
# Run Flask
# -----------------------------
@app.route("/")
def home():
    return "<h1>Fish Farm API Running</h1><p>WebSocket alerts active.</p>"


if __name__ == "__main__":
    print("ðŸš€ Flask + Kafka WebSocket server starting on port 5000...")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)