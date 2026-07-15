import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from azure.eventhub import EventData, EventHubProducerClient
from dotenv import load_dotenv


load_dotenv()


STORES = ["store_101", "store_102", "store_103"]
PRODUCTS = [
    {"product_id": "prod_001", "product_name": "Running Shoes", "category": "Footwear", "base_price": 89.99},
    {"product_id": "prod_002", "product_name": "Yoga Mat", "category": "Fitness", "base_price": 29.99},
    {"product_id": "prod_003", "product_name": "Water Bottle", "category": "Accessories", "base_price": 14.99},
    {"product_id": "prod_004", "product_name": "Backpack", "category": "Bags", "base_price": 59.99},
]


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def make_sales_event():
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 4)
    unit_price = round(product["base_price"] * random.uniform(0.85, 1.15), 2)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "SALE",
        "event_time": now_utc(),
        "store_id": random.choice(STORES),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "customer_id": f"cust_{random.randint(1000, 9999)}",
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": round(quantity * unit_price, 2),
        "payment_method": random.choice(["card", "cash", "wallet"]),
        "channel": random.choice(["in_store", "online"]),
    }


def make_inventory_event():
    product = random.choice(PRODUCTS)
    stock_on_hand = random.randint(0, 120)
    reorder_level = random.choice([10, 15, 20, 25])

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "INVENTORY",
        "event_time": now_utc(),
        "store_id": random.choice(STORES),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "stock_on_hand": stock_on_hand,
        "reorder_level": reorder_level,
        "stock_status": "LOW_STOCK" if stock_on_hand <= reorder_level else "NORMAL",
    }


def make_return_event():
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 2)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "RETURN",
        "event_time": now_utc(),
        "store_id": random.choice(STORES),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "customer_id": f"cust_{random.randint(1000, 9999)}",
        "quantity": quantity,
        "return_reason": random.choice(["damaged", "wrong_size", "late_delivery", "changed_mind"]),
        "refund_amount": round(product["base_price"] * quantity, 2),
    }


def send_event(producer, event):
    batch = producer.create_batch()
    batch.add(EventData(json.dumps(event)))
    producer.send_batch(batch)


def main():
    parser = argparse.ArgumentParser(description="Live retail event streaming app")
    parser.add_argument("--duration", type=int, default=120, help="Run duration in seconds. Use 0 for infinite.")
    parser.add_argument("--sleep", type=float, default=1.5, help="Seconds between events.")
    args = parser.parse_args()

    connection_string = os.getenv("EVENTHUB_PRODUCER_CONNECTION_STRING")

    sales_hub = os.getenv("SALES_EVENTHUB_NAME")
    inventory_hub = os.getenv("INVENTORY_EVENTHUB_NAME")
    return_hub = os.getenv("RETURN_EVENTHUB_NAME")

    if not connection_string:
        raise ValueError("Missing EVENTHUB_PRODUCER_CONNECTION_STRING in .env")

    producers = {
        "SALE": EventHubProducerClient.from_connection_string(connection_string, eventhub_name=sales_hub),
        "INVENTORY": EventHubProducerClient.from_connection_string(connection_string, eventhub_name=inventory_hub),
        "RETURN": EventHubProducerClient.from_connection_string(connection_string, eventhub_name=return_hub),
    }

    event_generators = [
        ("SALE", make_sales_event),
        ("INVENTORY", make_inventory_event),
        ("RETURN", make_return_event),
    ]

    print("Starting live retail stream...")
    print("Press Ctrl + C to stop.")

    start_time = time.time()
    total_sent = 0

    try:
        while True:
            if args.duration != 0 and time.time() - start_time >= args.duration:
                break

            event_type, generator = random.choices(
                event_generators,
                weights=[70, 20, 10],
                k=1,
            )[0]

            event = generator()
            send_event(producers[event_type], event)

            total_sent += 1
            print(f"Sent {event_type}: {event['event_id']}")

            time.sleep(args.sleep)

    except KeyboardInterrupt:
        print("\nStopped by user.")

    finally:
        for producer in producers.values():
            producer.close()

        print(f"Total events sent: {total_sent}")


if __name__ == "__main__":
    main()