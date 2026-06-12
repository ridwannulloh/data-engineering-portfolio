#!/usr/bin/env python3
"""Generate deterministic raw seed data for the DuckDB dbt demo.

Produces three CSVs under seeds/raw/ that emulate the ODS/Bronze layer that
would normally be landed by AWS DMS. Timestamps are stored in UTC (naive) plus
a `_dms_timestamp` audit column, mirroring a real CDC feed. Re-running this
script regenerates identical files (fixed seed), so the demo is reproducible.
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

OUT = Path(__file__).resolve().parent.parent / "seeds" / "raw"
OUT.mkdir(parents=True, exist_ok=True)

# Anchor "now" so recency segmentation in the customer mart is meaningful.
NOW = datetime(2026, 6, 12, 6, 0, 0)

COUNTRIES = ["ID", "MY", "SG", "TH", "PH", "VN", "IN", "AU", "US", "GB", "DE", "NL"]
TIERS = [1, 2, 3]
STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
STATUS_WEIGHTS = [0.08, 0.12, 0.15, 0.55, 0.10]

FIRST = ["Ayu", "Budi", "Citra", "Dewi", "Eko", "Fitri", "Gilang", "Hana",
         "Indra", "Joko", "Kiki", "Lina", "Mia", "Nanda", "Omar", "Putri"]
LAST = ["Santoso", "Wijaya", "Pratama", "Lestari", "Halim", "Saputra",
        "Nugroho", "Kusuma", "Hartono", "Permata", "Yusuf", "Rahmawati"]

PRODUCTS = [
    ("P01", "Aurora Wireless Earbuds", "ELECTRONICS", "AUDIO", 59.90),
    ("P02", "Nimbus Mechanical Keyboard", "ELECTRONICS", "PERIPHERALS", 89.00),
    ("P03", "Lumen 4K Webcam", "ELECTRONICS", "PERIPHERALS", 74.50),
    ("P04", "Terra Insulated Bottle", "HOME", "KITCHEN", 24.00),
    ("P05", "Drift Cotton Hoodie", "APPAREL", "OUTERWEAR", 45.00),
    ("P06", "Pulse Running Shoes", "APPAREL", "FOOTWEAR", 119.00),
    ("P07", "Glow Desk Lamp", "HOME", "LIGHTING", 38.75),
    ("P08", "Atlas Travel Backpack", "ACCESSORIES", "BAGS", 95.00),
]


def fmt(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def write_customers():
    rows = []
    for i in range(1, 13):
        registered = NOW - timedelta(days=random.randint(120, 900))
        rows.append({
            "customer_id": f"C{i:03d}",
            "email": f"{FIRST[i % len(FIRST)].lower()}.{LAST[i % len(LAST)].lower()}{i}@example.com",
            "country_code": COUNTRIES[i % len(COUNTRIES)],
            "first_name": FIRST[i % len(FIRST)],
            "last_name": LAST[i % len(LAST)],
            "customer_tier": random.choice(TIERS),
            "registered_at": fmt(registered),
            "_dms_timestamp": fmt(NOW - timedelta(minutes=random.randint(5, 90))),
        })
    _dump("customers.csv", rows)
    return [r["customer_id"] for r in rows]


def write_products():
    rows = []
    for pid, name, cat, sub, price in PRODUCTS:
        rows.append({
            "product_id": pid,
            "product_name": name,
            "category": cat,
            "subcategory": sub,
            "unit_price": f"{price:.4f}",
            "currency_code": "USD",
            "is_active": "true",
            "_dms_timestamp": fmt(NOW - timedelta(minutes=random.randint(5, 90))),
        })
    _dump("products.csv", rows)
    return [r["product_id"] for r in rows]


def write_orders(customer_ids, product_ids):
    rows = []
    for i in range(1, 61):
        created = NOW - timedelta(days=random.randint(0, 240),
                                  hours=random.randint(0, 23),
                                  minutes=random.randint(0, 59))
        product = random.choice(product_ids)
        unit_price = next(p[4] for p in PRODUCTS if p[0] == product)
        qty = random.randint(1, 4)
        rows.append({
            "order_id": f"O{i:04d}",
            "customer_id": random.choice(customer_ids),
            "product_id": product,
            "order_status": random.choices(STATUSES, STATUS_WEIGHTS)[0],
            "created_at": fmt(created),
            "total_amount": f"{unit_price * qty:.2f}",
            "currency_code": "USD",
            "_dms_timestamp": fmt(created + timedelta(minutes=random.randint(1, 30))),
        })
    rows.sort(key=lambda r: r["created_at"])
    _dump("orders.csv", rows)


def _dump(filename, rows):
    path = OUT / filename
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows):>3} rows -> {path.relative_to(OUT.parent.parent)}")


if __name__ == "__main__":
    cust = write_customers()
    prod = write_products()
    write_orders(cust, prod)
