from kafka import KafkaProducer
import json

# Kafka broker configuration
KAFKA_BROKER = 'localhost:9092'  
TOPIC = 'color_detection'  # Kafka topic to send detection results

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def produce_detection_result(label, confidence, top_colors, pants_colors, dominant_colors):
    """Produce detection result to Kafka."""
    detection_result = {
        'label': label,
        'confidence': confidence,
        'top_colors': top_colors,
        'pants_colors': pants_colors,
        'dominant_colors': dominant_colors
    }
    producer.send(TOPIC, value=detection_result)
    producer.flush()

# function to be used in main.py
def send_detection_results_to_kafka(label, confidence, top_colors, pants_colors, dominant_colors):
    produce_detection_result(label, confidence, top_colors, pants_colors, dominant_colors)
