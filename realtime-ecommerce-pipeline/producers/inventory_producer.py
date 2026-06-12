"""
producers/inventory_producer.py
Simulates warehouse inventory updates flowing into Kafka.

Events:
  - restock    : product quantity increased
  - sale       : quantity decremented on order fulfilment
  - adjustment : manual stock correction
"""

import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS, PRODUCT_IDS, CATEGORIES

WAREHOUSE_IDS = [f"WH-{i:02d}" for i in range(1, 6)]   # 5 warehouses

# Seed initial stock levels per product
STOCK_LEVELS: dict[str, int] = {pid: random.randint(50, 500) for pid in PRODUCT_IDS}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_inventory_event(product_id: str) -> dict:
    current_qty = STOCK_LEVELS[product_id]
    event_type  = random.choices(
        ["restock", "sale", "adjustment"],
        weights=[0.25, 0.65, 0.10],
    )[0]

    if event_type == "restock":
        delta = random.randint(50, 300)
    elif event_type == "sale":
        delta = -random.randint(1, min(5, current_qty or 1))
    else:
        delta = random.randint(-20, 20)

    new_qty = max(0, current_qty + delta)
    STOCK_LEVELS[product_id] = new_qty

    return {
        "event_type":    event_type,
        "product_id":    product_id,
        "warehouse_id":  random.choice(WAREHOUSE_IDS),
        "category":      random.choice(CATEGORIES),
        "delta_quantity": delta,
        "quantity_after": new_qty,
        "low_stock_flag": new_qty < 20,
        "stockout_flag":  new_qty == 0,
        "recorded_at":   now_iso(),
    }


def delivery_report(err, msg):
    if err:
        print(f"[INV PRODUCER] Delivery failed: {err}")


def run(events_per_minute: int = 60, duration_seconds: int = None):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    topic    = TOPICS["inventory"]
    interval = 60.0 / events_per_minute
    start    = time.time()
    count    = 0

    print(f"[INV PRODUCER] Starting @ {events_per_minute} events/min")

    try:
        while True:
            product_id = random.choice(PRODUCT_IDS)
            event      = make_inventory_event(product_id)

            producer.produce(
                topic,
                key=product_id,
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)
            count += 1

            if duration_seconds and (time.time() - start) >= duration_seconds:
                break
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[INV PRODUCER] Interrupted.")
    finally:
        producer.flush()
        print(f"[INV PRODUCER] Done. Events produced: {count}")


if __name__ == "__main__":
    run(events_per_minute=60)
