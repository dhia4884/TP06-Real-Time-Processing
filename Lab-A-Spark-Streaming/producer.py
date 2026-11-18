
import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

# Connect to the existing Kafka broker
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

TOPIC = "campus-events"

event_types = [
    "login",
    "library_entry",
    "course_registration",
    "wifi_connection"
]

print("Kafka Producer started...")
print("Sending campus events. Press Ctrl+C to stop.")

try:
    while True:
        event = {
            "studentId": random.randint(1000, 1100),
            "eventType": random.choice(event_types),
            "eventTime": datetime.now(timezone.utc).isoformat()
        }

        producer.send(TOPIC, value=event)

        print(f"Sent: {event}")

        time.sleep(0.01)

except KeyboardInterrupt:
    print("\nProducer stopped.")

finally:
    producer.flush()
    producer.close()
