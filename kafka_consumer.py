import sqlite3
from kafka import KafkaConsumer
import json

# Kafka consumer configuration
KAFKA_BROKER = 'localhost:9092'  
TOPIC = 'detection_results'  # Kafka topic to consume detection results

# SQLite database connection
conn = sqlite3.connect('/local/workspace/tappas/apps/h8/gstreamer/general/instance_segmentation/detections.db')
cursor = conn.cursor()

# Ensure table is created
cursor.execute('''
    CREATE TABLE IF NOT EXISTS detections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        label TEXT,
        confidence REAL,
        top_colors TEXT,
        pants_colors TEXT,
        dominant_colors TEXT
    )
''')
conn.commit()

# Create Kafka consumer
consumer = KafkaConsumer(TOPIC, bootstrap_servers=KAFKA_BROKER, value_deserializer=lambda v: json.loads(v.decode('utf-8')))

def consume_and_store_detection_results():
    """Consume detection results from Kafka and store in SQLite database."""
    for message in consumer:
        try:
            detection_result = message.value
            label = detection_result.get('label')
            confidence = detection_result.get('confidence')
            top_colors = detection_result.get('top_colors', "")
            pants_colors = detection_result.get('pants_colors', "")
            dominant_colors = detection_result.get('dominant_colors', "")

            cursor.execute('''
                INSERT INTO detections (label, confidence, top_colors, pants_colors, dominant_colors)
                VALUES (?, ?, ?, ?, ?)
            ''', (label, confidence, top_colors, pants_colors, dominant_colors))
            conn.commit()

            print(f"Stored detection result for {label} with confidence {confidence}")

        except Exception as e:
            print(f"Error processing message: {str(e)}")

# function to be used in main.py
def start_consumer():
    consume_and_store_detection_results()
