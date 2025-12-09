# consumer.py
from kafka import KafkaConsumer
import json

TOPICS = ["dbserver1.public.users", "dbserver1.public.orders"]

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    key_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
    consumer_timeout_ms=10000
)

print("Listening for messages on:", TOPICS)
for msg in consumer:
    print("Topic:", msg.topic)
    print(json.dumps(msg.value, indent=2))
    print("-" * 80)
