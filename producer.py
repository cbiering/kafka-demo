import json
import uuid
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered {msg.value().decode('utf-8')}")

producer = Producer({'bootstrap.servers': 'localhost:9092'})

order = {
    "order_id": str(uuid.uuid4()),
    "user": "chris",
    "item": "keyboard",
    "quantity": 1
}

value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="orders",
    value=value,
    callback=delivery_report
)

producer.flush()

