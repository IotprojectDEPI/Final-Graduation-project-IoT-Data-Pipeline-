# alert_producer.py
import sqlite3
import pandas as pd
import time
from kafka import KafkaProducer
import json

DB_PATH = "Readings.db"
TOPIC = "iot-sensor-data"

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_latest_rows():
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM Readings ORDER BY Timestamp DESC LIMIT 11"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df[::-1]  # reverse to restore chronological order

def check_alerts(df):
    alerts = []
    for _, row in df.iterrows():
        for col in df.columns:
            if any(flag in str(row[col]) for flag in ['F+', 'F++', 'F-', 'F--']):
                alerts.append({
                    "timestamp": row["Timestamp"],
                    "fish": row.get("fish", "Unknown"),
                    "sensor": col,
                    "value": str(row[col]),
                    "alert": "CRITICAL THRESHOLD BREACH"
                })
    return alerts

print("Kafka Alert Producer running... (Press Ctrl+C to stop)")

try:
    while True:
        latest_rows = get_latest_rows()
        alerts = check_alerts(latest_rows)
        
        if alerts:
            for alert in alerts:
                producer.send(TOPIC, value=alert)
                print(f"⚠️ Alert sent: {alert}")
        else:
            print("No critical alerts found in latest batch.")

        time.sleep(5)  # check every 5 seconds
except KeyboardInterrupt:
    print("\nProducer stopped.")
####################################################################