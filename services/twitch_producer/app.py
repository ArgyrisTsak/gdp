import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPIC = "twitch-events"

conf = {
    "bootstrap.servers": KAFKA_BROKER
}

producer = Producer(conf)

def generate_mock_event():
    return {
        "game_id": random.randint(1, 5),
        "viewer_count": random.randint(1000, 100000),
        "timestamp": datetime.utcnow().isoformat()
    }

print("Starting Twitch mock producer...")

while True:
    event = generate_mock_event()
    producer.produce(
        TOPIC,
        key=str(event["game_id"]),
        value=json.dumps(event)
    )
    producer.flush()
    print(f"Produced: {event}")
    time.sleep(5)
