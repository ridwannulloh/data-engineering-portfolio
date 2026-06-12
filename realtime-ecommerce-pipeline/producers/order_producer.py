"""
producers/order_producer.py
Simulates a realistic e-commerce order stream into Kafka.

Events produced:
  - Order placed  (new order with items)
  - Status update (payment → shipped → delivered, or cancelled)
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from config import (
    KAFKA_BOOTSTRAP_SERVERS, TOPICS,
    PRODUCT_IDS, CUSTOMER_IDS, CATEGORIES,
    ORDER_STATUSES,
)

# ── Helpers ────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_order_id() -> str:
    return f"ORD-{uuid.uuid4().hex[:12].upper()}"


def make_order_items(order_id: str) -> list[dict]:
    n_items = random.randint(1, 5)
    items = []
    for _ in range(n_items):
        product_id = random.choice(PRODUCT_IDS)
        qty        = random.randint(1, 4)
        unit_price = round(random.uniform(5.0, 499.99), 2)
        items.append({
            "order_id":   order_id,
            "product_id": product_id,
            "category":   random.choice(CATEGORIES),
            "quantity":   qty,
            "unit_price": unit_price,
            "subtotal":   round(qty * unit_price, 2),
        })
    return items


def make_order_placed(order_id: str, customer_id: str) -> dict:
    items      = make_order_items(order_id)
    total      = round(sum(i["subtotal"] for i in items), 2)
    return {
        "event_type":   "order_placed",
        "order_id":     order_id,
        "customer_id":  customer_id,
        "status":       "placed",
        "total_amount": total,
        "currency":     "USD",
        "channel":      random.choice(["web", "mobile", "partner_api"]),
        "items":        items,
        "created_at":   now_iso(),
    }


def make_status_update(order_id: str, customer_id: str, status: str) -> dict:
    return {
        "event_type":  "order_status_update",
        "order_id":    order_id,
        "customer_id": customer_id,
        "status":      status,
        "updated_at":  now_iso(),
    }


# ── Delivery callback ──────────────────────────────────────────────────────

def delivery_report(err, msg):
    if err:
        print(f"[PRODUCER] Delivery failed: {err}")
    else:
        print(f"[PRODUCER] Delivered → {msg.topic()} [{msg.partition()}] offset={msg.offset()}")


# ── Main loop ──────────────────────────────────────────────────────────────

def run(orders_per_minute: int = 30, duration_seconds: int = None):
    """
    Produce order events continuously.

    Args:
        orders_per_minute: Target throughput for new orders.
        duration_seconds:  Run for N seconds then stop. None = run forever.
    """
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    topic_orders = TOPICS["orders"]
    topic_items  = TOPICS["order_items"]

    interval   = 60.0 / orders_per_minute
    start_time = time.time()
    produced   = 0

    # Track open orders so we can emit status updates
    open_orders: list[tuple[str, str]] = []  # (order_id, customer_id)

    print(f"[PRODUCER] Starting order producer @ {orders_per_minute} orders/min")

    try:
        while True:
            # ── Emit a new order ──────────────────────────────────────────
            order_id    = make_order_id()
            customer_id = random.choice(CUSTOMER_IDS)
            order_evt   = make_order_placed(order_id, customer_id)

            producer.produce(
                topic_orders,
                key=order_id,
                value=json.dumps(order_evt),
                callback=delivery_report,
            )

            # Also publish each item separately for granular joins
            for item in order_evt["items"]:
                producer.produce(
                    topic_items,
                    key=order_id,
                    value=json.dumps(item),
                )

            open_orders.append((order_id, customer_id))
            produced += 1

            # ── Randomly progress an open order's status ──────────────────
            if open_orders and random.random() < 0.4:
                oid, cid = random.choice(open_orders)

                # 10 % chance of cancellation, else move forward
                if random.random() < 0.10:
                    status = "cancelled"
                    open_orders = [(o, c) for o, c in open_orders if o != oid]
                else:
                    idx     = random.randint(1, len(ORDER_STATUSES) - 2)
                    status  = ORDER_STATUSES[idx]
                    if status == "delivered":
                        open_orders = [(o, c) for o, c in open_orders if o != oid]

                update_evt = make_status_update(oid, cid, status)
                producer.produce(
                    topic_orders,
                    key=oid,
                    value=json.dumps(update_evt),
                    callback=delivery_report,
                )

            producer.poll(0)

            if duration_seconds and (time.time() - start_time) >= duration_seconds:
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[PRODUCER] Interrupted by user.")
    finally:
        producer.flush()
        print(f"[PRODUCER] Done. Total orders produced: {produced}")


if __name__ == "__main__":
    run(orders_per_minute=20)
