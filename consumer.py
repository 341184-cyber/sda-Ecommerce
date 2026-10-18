import json
from datetime import datetime, timezone

from kafka import KafkaConsumer

from database import orders_collection


TOPIC = "ecommerce-orders"
BOOTSTRAP_SERVERS = "localhost:9092"


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="ecommerce-analytics",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)

print("======================================")
print("MongoDB Kafka Consumer Started")
print("======================================")
print(f"Kafka topic: {TOPIC}")
print("Waiting for orders...")
print()


for message in consumer:

    event = message.value

    # Convert timestamp from string to Python datetime
    event["order_purchase_timestamp"] = datetime.fromisoformat(
        event["order_purchase_timestamp"]
    )

    # Store processing time as a MongoDB Date
    event["stream_processed_at"] = datetime.now(timezone.utc)

    orders_collection.update_one(
        {"order_id": event["order_id"]},
        {"$set": event},
        upsert=True
    )

    print(
        f"Stored order: {event['order_id']} | "
        f"Status: {event['order_status']} | "
        f"Revenue: {event['total_price']}"
    )