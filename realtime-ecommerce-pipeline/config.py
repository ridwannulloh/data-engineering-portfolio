"""
Shared configuration — Kafka topics, S3 paths, schema definitions.

Connection endpoints are environment-driven so the exact same code runs both
on the host (defaults point at localhost) and inside Docker Compose (compose
overrides them with the service hostnames `kafka:29092` / `minio:9000`).
"""

import os

# ── Kafka ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPICS = {
    "orders":         "ecommerce.orders",
    "order_items":    "ecommerce.order_items",
    "inventory":      "ecommerce.inventory_updates",
    "clickstream":    "ecommerce.clickstream",
}

# ── MinIO / S3 ─────────────────────────────────────────────────────────────
MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET            = os.getenv("MINIO_BUCKET", "ecommerce-lakehouse")

S3_PATHS = {
    "bronze_orders":     "bronze/orders/",
    "bronze_inventory":  "bronze/inventory/",
    "bronze_clickstream":"bronze/clickstream/",
    "silver_orders":     "silver/orders_enriched/",
    "silver_inventory":  "silver/inventory_snapshot/",
    "gold_gmv":          "gold/gmv_hourly/",
    "gold_stockout":     "gold/stockout_risk/",
    "gold_rfm":          "gold/customer_rfm/",
}

# ── Order status machine ───────────────────────────────────────────────────
ORDER_STATUSES = [
    "placed", "payment_confirmed", "processing",
    "shipped", "delivered", "cancelled", "returned",
]

# ── Product catalogue (simulated) ─────────────────────────────────────────
CATEGORIES   = ["Electronics", "Apparel", "Home & Garden", "Sports", "Beauty"]
PRODUCT_IDS  = [f"PROD_{i:04d}" for i in range(1, 201)]   # 200 SKUs
CUSTOMER_IDS = [f"CUST_{i:06d}" for i in range(1, 5001)]  # 5 000 customers
