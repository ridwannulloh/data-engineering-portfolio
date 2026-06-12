"""
producers/clickstream_producer.py
Simulates user clickstream events (page views, add-to-cart, checkout starts).
Used downstream for conversion funnel and demand-signal analytics.
"""

import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS, PRODUCT_IDS, CUSTOMER_IDS

EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "remove_from_cart",
               "checkout_start", "checkout_complete", "search"]
DEVICES     = ["desktop", "mobile", "tablet"]
REFERRERS   = ["organic_search", "paid_search", "social", "email", "direct", "affiliate"]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_click_event() -> dict:
    event_type  = random.choices(
        EVENT_TYPES,
        weights=[0.30, 0.25, 0.15, 0.05, 0.10, 0.05, 0.10],
    )[0]

    event = {
        "event_type":  event_type,
        "session_id":  f"sess-{random.randint(100000, 999999)}",
        "customer_id": random.choice(CUSTOMER_IDS) if random.random() > 0.35 else None,  # 35% anonymous
        "device":      random.choice(DEVICES),
        "referrer":    random.choice(REFERRERS),
        "timestamp":   now_iso(),
    }

    if event_type in ("product_view", "add_to_cart", "remove_from_cart"):
        event["product_id"] = random.choice(PRODUCT_IDS)

    if event_type == "search":
        event["search_query"] = random.choice([
            "running shoes", "bluetooth headphones", "coffee maker",
            "yoga mat", "laptop stand", "winter jacket",
        ])

    return event


def delivery_report(err, msg):
    if err:
        print(f"[CLICK PRODUCER] Delivery failed: {err}")


def run(events_per_minute: int = 120, duration_seconds: int = None):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    topic    = TOPICS["clickstream"]
    interval = 60.0 / events_per_minute
    start    = time.time()
    count    = 0

    print(f"[CLICK PRODUCER] Starting @ {events_per_minute} events/min")

    try:
        while True:
            event = make_click_event()
            producer.produce(
                topic,
                key=event.get("session_id"),
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)
            count += 1

            if duration_seconds and (time.time() - start) >= duration_seconds:
                break
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[CLICK PRODUCER] Interrupted.")
    finally:
        producer.flush()
        print(f"[CLICK PRODUCER] Done. Events produced: {count}")


if __name__ == "__main__":
    run(events_per_minute=120)
