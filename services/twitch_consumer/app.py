import os
import json
import pandas as pd
from confluent_kafka import Consumer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPIC = "twitch-events"
BRONZE_DIR = "/data/bronze/twitch"

os.makedirs(BRONZE_DIR, exist_ok=True)

conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "bronze-consumer",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])

print("Starting Twitch Bronze consumer...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue

        data = json.loads(msg.value().decode("utf-8"))
        filename = f"{BRONZE_DIR}/twitch_{data['timestamp'].replace(':','-')}.json"
        with open(filename, "w") as f:
            json.dump(data, f)

        print("Saved event to", filename)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
