import json
import time
import pandas as pd

from kafka import KafkaProducer


CSV_FILE = "data/processed/orders_stream.csv"
KAFKA_SERVER = "localhost:9092"
TOPIC = "ecommerce-orders"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value:
        json.dumps(value).encode("utf-8")
)


df = pd.read_csv(CSV_FILE)


print(f"Loaded {len(df):,} orders.")
print("Starting stream...")
print("Press CTRL+C to stop.")


for _, row in df.iterrows():

    event = {
        "order_id": str(row["order_id"]),
        "customer_id": str(row["customer_id"]),
        "order_status": str(row["order_status"]),
        "order_purchase_timestamp": str(
            row["order_purchase_timestamp"]
        ),
        "total_price": float(row["total_price"]),
        "total_freight": float(row["total_freight"]),
        "item_count": int(row["item_count"]),
        "customer_city": str(row["customer_city"]),
        "customer_state": str(row["customer_state"]),
        "product_category": str(
            row["product_category_name"]
        )
    }

    producer.send(
        TOPIC,
        value=event
    )

    print(
        f"Sent: {event['order_id']} | "
        f"₹{event['total_price']:.2f} | "
        f"{event['product_category']}"
    )

    producer.flush()

    # Simulate real-time arrival
    time.sleep(1)