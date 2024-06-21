from flask import Flask, request, jsonify
from threading import Thread
import subprocess
import os
import signal
import sqlite3
import atexit
from apscheduler.schedulers.background import BackgroundScheduler
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import json
import os

app = Flask(__name__)
#app.secret_key = 'CNJ31AS*($DKLD234)'

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'detections'

# Path to the TAPPAS workspace
TAPPAS_WORKSPACE = "/local/workspace/tappas"

# Path to the instance segmentation script
INSTANCE_SEGMENTATION_SCRIPT = os.path.join(TAPPAS_WORKSPACE, "apps/h8/gstreamer/general/instance_segmentation/instance_segmentation.sh")

# Database path
DB_PATH = os.path.join(TAPPAS_WORKSPACE, "apps/h8/gstreamer/general/instance_segmentation/detections.db")

# Process holder
pipeline_process = None

#Kafka producer setup
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Database setup
def setup_database():
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    table = """CREATE TABLE IF NOT EXISTS detections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT,
            confidence REAL,
            x_min INTEGER,
            y_min INTEGER,
            x_max INTEGER,
            y_max INTEGER,
            mask_shape STRING,
            color TEXT);"""

    session_table = """CREATE TABLE IF NOT EXISTS Sessions(token TEXT);"""

    cursor.execute(table)
    cursor.execute(session_table)
    connection.commit()
    cursor.close()
    connection.close()

setup_database()

@app.route("/")
def index():
    """Render homepage template on root route."""
    return "Instance Segmentation API"

@app.route('/start_detection', methods=["POST"])
def start_detection():
    """Start the instance segmentation pipeline."""
    global pipeline_process

    video_source = request.json.get('video_source', '/home/service/develop/HailoColorec/tappass/apps/h8/gstreamer/general/instance_segmentation/resources/instance_segmentation.mp4')
    show_fps = request.json.get('show_fps', False)
    args = [INSTANCE_SEGMENTATION_SCRIPT, '-i', video_source]

    if show_fps:
        args.append('--show-fps')

    try:
        pipeline_process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return jsonify({"message": "Pipeline started", "pid": pipeline_process.pid})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/stop_detection', methods=["POST"])
def stop_detection():
    """Stop the instance segmentation pipeline."""
    global pipeline_process

    if pipeline_process:
        pipeline_process.terminate()
        pipeline_process.wait()
        pipeline_process = None
        return jsonify({"message": "Pipeline stopped"})
    else:
        return jsonify({"error": "No pipeline running"}), 400
    
@app.route('/send_to_kafka', methods=["POST"])
def send_to_kafka():
    """Send data to Kafka."""
    data = request.json
    try:
        producer.produce(KAFKA_TOPIC, key=str(data['id']), value=json.dumps(data))
        producer.flush()
        return jsonify({"message": "Data sent to Kafka"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
   
def fetch_and_send_results():
    """Fetch results from the database and send them to Kafka."""
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    cursor.execute("SELECT * FROM detections")
    rows = cursor.fetchall()

    for row in rows:
        data = {
            "id": row[0],
            "label": row[1],
            "confidence": row[2],
            "bbox": {
                "x_min": row[3],
                "y_min": row[4],
                "x_max": row[5],
                "y_max": row[6]
            },
            "mask_shape": row[7],
            "color": row[8]
        }

        try:
            producer.produce(KAFKA_TOPIC, key=str(data['id']), value=json.dumps(data))
            producer.flush()
        except Exception as e:
            print(f"Failed to send data to Kafka: {str(e)}")

    cursor.execute("DELETE FROM detections")
    connection.commit()
    cursor.close()
    connection.close()

def shutdown_server():
    if pipeline_process:
        pipeline_process.terminate()
        pipeline_process.wait()

atexit.register(shutdown_server)

# Scheduler for periodic tasks
scheduler = BackgroundScheduler()
scheduler.add_job(setup_database, 'interval', minutes=15)
scheduler.start()

# Driver function
if __name__ == "__main__":
    app.run(debug=True, use_reloader=False, host="127.0.0.1", port=5000)
