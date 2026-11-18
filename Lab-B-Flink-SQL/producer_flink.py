
import json
import time
import random
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "department-checkins"

departments = [
    "Computer Science",
    "Mathematics",
    "Physics",
    "Biology"
]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("Flink Lab - Kafka Producer started")
print("Sending department check-ins...")
print("Press Ctrl+C to stop")

try:
    while True:
        event = {
            "department": random.choice(departments),
            "event_time": datetime.now(timezone.utc).isoformat(
                timespec="milliseconds"
            ).replace("+00:00", "Z")
        }

        producer.send(TOPIC, value=event)
        print(f"Sent: {event}")

        time.sleep(1)

except KeyboardInterrupt:
    print("\nProducer stopped.")

finally:
    producer.flush()
    producer.close()
