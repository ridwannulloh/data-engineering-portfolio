"""
pipelines/bronze_to_silver.py
──────────────────────────────
Batch micro-pipeline: Bronze (raw JSON) → Silver (cleaned + enriched Parquet).

Runs on a schedule (e.g., every 15 minutes via Airflow or cron).

Transformations applied:
  Orders:
    - Parse ISO timestamps → proper datetime
    - Normalise channel values
    - Add derived columns: order_hour, order_day_of_week, is_weekend
    - Null-check and reject malformed records to a quarantine path

  Inventory:
    - Deduplicate by (product_id, warehouse_id, recorded_at)
    - Classify stock_risk_level: OK / LOW / CRITICAL / STOCKOUT
    - Add rolling_24h_delta placeholder column
"""

import json
import os
import io
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, BUCKET,
)

# ── S3 helper ──────────────────────────────────────────────────────────────

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )


def list_objects(s3, prefix: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return keys


def read_ndjson(s3, key: str) -> list[dict]:
    resp    = s3.get_object(Bucket=BUCKET, Key=key)
    content = resp["Body"].read().decode("utf-8")
    records = []
    for line in content.splitlines():
        line = line.strip()
        if line:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return records


def write_parquet(s3, df: pd.DataFrame, s3_key: str):
    table  = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=buffer.read())
    print(f"[SILVER] Wrote {len(df)} rows → s3://{BUCKET}/{s3_key}")


def write_quarantine(s3, records: list[dict], label: str):
    if not records:
        return
    key     = f"quarantine/{label}/{uuid.uuid4().hex}.json"
    payload = "\n".join(json.dumps(r) for r in records)
    s3.put_object(Bucket=BUCKET, Key=key, Body=payload.encode())
    print(f"[QUARANTINE] {len(records)} rejected records → s3://{BUCKET}/{key}")


# ── Order transformation ───────────────────────────────────────────────────

REQUIRED_ORDER_FIELDS = {"order_id", "customer_id", "total_amount", "created_at"}

def transform_orders(records: list[dict]) -> tuple[pd.DataFrame, list[dict]]:
    valid    = []
    rejected = []

    for r in records:
        # Skip non-placement events
        if r.get("event_type") != "order_placed":
            continue

        # Reject if required fields missing
        if not REQUIRED_ORDER_FIELDS.issubset(r.keys()):
            rejected.append({**r, "_reject_reason": "missing_required_fields"})
            continue

        try:
            created_at = datetime.fromisoformat(r["created_at"])
        except (ValueError, TypeError):
            rejected.append({**r, "_reject_reason": "invalid_timestamp"})
            continue

        valid.append({
            "order_id":         r["order_id"],
            "customer_id":      r["customer_id"],
            "status":           r.get("status", "unknown"),
            "channel":          r.get("channel", "unknown").lower().strip(),
            "total_amount":     float(r.get("total_amount", 0)),
            "currency":         r.get("currency", "USD"),
            "item_count":       len(r.get("items", [])),
            "created_at":       created_at,
            "order_hour":       created_at.hour,
            "order_day_of_week": created_at.weekday(),   # 0=Mon … 6=Sun
            "is_weekend":       created_at.weekday() >= 5,
            "_ingested_at":     r.get("_ingested_at"),
        })

    df = pd.DataFrame(valid) if valid else pd.DataFrame()
    return df, rejected


# ── Inventory transformation ───────────────────────────────────────────────

def stock_risk_level(qty: int) -> str:
    if qty == 0:
        return "STOCKOUT"
    elif qty < 10:
        return "CRITICAL"
    elif qty < 20:
        return "LOW"
    return "OK"


def transform_inventory(records: list[dict]) -> pd.DataFrame:
    rows = []
    for r in records:
        if "product_id" not in r or "quantity_after" not in r:
            continue
        rows.append({
            "product_id":       r["product_id"],
            "warehouse_id":     r.get("warehouse_id", "unknown"),
            "category":         r.get("category", "unknown"),
            "event_type":       r.get("event_type", "unknown"),
            "delta_quantity":   int(r.get("delta_quantity", 0)),
            "quantity_after":   int(r.get("quantity_after", 0)),
            "stock_risk_level": stock_risk_level(int(r.get("quantity_after", 0))),
            "low_stock_flag":   bool(r.get("low_stock_flag", False)),
            "stockout_flag":    bool(r.get("stockout_flag", False)),
            "recorded_at":      r.get("recorded_at"),
            "_ingested_at":     r.get("_ingested_at"),
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # Deduplicate — keep latest per product+warehouse
    df = (
        df.sort_values("recorded_at", ascending=False)
          .drop_duplicates(subset=["product_id", "warehouse_id", "recorded_at"])
          .reset_index(drop=True)
    )
    return df


# ── Orchestration ──────────────────────────────────────────────────────────

def run():
    s3 = s3_client()
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    # ── Process Orders ─────────────────────────────────────────────────────
    print("[SILVER] Processing Orders bronze layer...")
    order_keys = list_objects(s3, "bronze/orders/")
    all_orders = []
    for key in order_keys:
        all_orders.extend(read_ndjson(s3, key))

    if all_orders:
        df_orders, rejected = transform_orders(all_orders)
        write_quarantine(s3, rejected, "orders")
        if not df_orders.empty:
            write_parquet(s3, df_orders, f"silver/orders_enriched/run={run_ts}/data.parquet")

    # ── Process Inventory ──────────────────────────────────────────────────
    print("[SILVER] Processing Inventory bronze layer...")
    inv_keys    = list_objects(s3, "bronze/inventory/")
    all_inv     = []
    for key in inv_keys:
        all_inv.extend(read_ndjson(s3, key))

    if all_inv:
        df_inv = transform_inventory(all_inv)
        if not df_inv.empty:
            write_parquet(s3, df_inv, f"silver/inventory_snapshot/run={run_ts}/data.parquet")

    print("[SILVER] Pipeline complete.")


if __name__ == "__main__":
    run()
