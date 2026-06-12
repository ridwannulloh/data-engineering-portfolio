"""
pipelines/silver_to_gold.py
────────────────────────────
Silver → Gold analytical layer.

Gold tables produced:
  1. gmv_hourly         — hourly GMV by channel, with WoW comparison placeholder
  2. customer_rfm       — RFM segmentation (Recency / Frequency / Monetary)
                          Economics differentiator: quintile-based scoring

Run after bronze_to_silver.py completes.
"""

import io
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, BUCKET

# ── S3 helpers ─────────────────────────────────────────────────────────────

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )


def read_all_parquet(s3, prefix: str) -> pd.DataFrame:
    paginator = s3.get_paginator("list_objects_v2")
    frames    = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                resp   = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                buf    = io.BytesIO(resp["Body"].read())
                frames.append(pd.read_parquet(buf))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def write_parquet(s3, df: pd.DataFrame, key: str):
    table  = pa.Table.from_pandas(df, preserve_index=False)
    buf    = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.read())
    print(f"[GOLD] Wrote {len(df)} rows → s3://{BUCKET}/{key}")


# ── Gold 1: Hourly GMV by channel ─────────────────────────────────────────

def build_gmv_hourly(df_orders: pd.DataFrame) -> pd.DataFrame:
    if df_orders.empty:
        return pd.DataFrame()

    df = df_orders.copy()
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["window_hour"] = df["created_at"].dt.floor("h")

    gmv = (
        df.groupby(["window_hour", "channel"])
        .agg(
            total_gmv    = ("total_amount", "sum"),
            order_count  = ("order_id",     "count"),
            unique_customers = ("customer_id", "nunique"),
        )
        .reset_index()
    )

    gmv["avg_order_value"] = (gmv["total_gmv"] / gmv["order_count"]).round(2)
    gmv["total_gmv"]       = gmv["total_gmv"].round(2)

    # Placeholder for WoW: requires 7-day lookback (would join historical gold)
    gmv["wow_gmv_delta_pct"] = None

    gmv["computed_at"] = datetime.now(timezone.utc).isoformat()
    return gmv


# ── Gold 2: Customer RFM Segmentation ─────────────────────────────────────
# Quintile-based scoring (1–5 per dimension, 5 = best).
# Economics framing: treats each customer as a revenue-generating asset.

SEGMENT_MAP = {
    (5, 5): "Champions",
    (5, 4): "Loyal Customers",
    (4, 5): "Loyal Customers",
    (4, 4): "Loyal Customers",
    (5, 3): "Potential Loyalist",
    (3, 5): "Potential Loyalist",
    (5, 1): "Recent Customers",
    (5, 2): "Recent Customers",
    (3, 3): "Needs Attention",
    (2, 3): "About to Sleep",
    (2, 2): "About to Sleep",
    (1, 5): "Cant Lose Them",
    (1, 4): "Cant Lose Them",
    (1, 3): "At Risk",
    (2, 1): "Hibernating",
    (1, 1): "Lost",
    (1, 2): "Lost",
}


def get_segment(r_score: int, fm_score: int) -> str:
    return SEGMENT_MAP.get((r_score, fm_score), "General")


def build_rfm(df_orders: pd.DataFrame, snapshot_date: datetime = None) -> pd.DataFrame:
    if df_orders.empty:
        return pd.DataFrame()

    if snapshot_date is None:
        snapshot_date = datetime.now(timezone.utc)

    df = df_orders.copy()
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)

    # ── Raw RFM metrics ────────────────────────────────────────────────────
    rfm = (
        df.groupby("customer_id")
        .agg(
            last_order_date = ("created_at",    "max"),
            frequency       = ("order_id",       "count"),
            monetary        = ("total_amount",    "sum"),
        )
        .reset_index()
    )

    rfm["recency_days"] = (snapshot_date - rfm["last_order_date"]).dt.days

    # ── Quintile scoring ───────────────────────────────────────────────────
    # Recency: lower days = better → rank ascending then invert
    rfm["r_score"] = pd.qcut(rfm["recency_days"].rank(method="first"),
                              q=5, labels=[5, 4, 3, 2, 1]).astype(int)

    rfm["f_score"] = pd.qcut(rfm["frequency"].rank(method="first"),
                              q=5, labels=[1, 2, 3, 4, 5]).astype(int)

    rfm["m_score"] = pd.qcut(rfm["monetary"].rank(method="first"),
                              q=5, labels=[1, 2, 3, 4, 5]).astype(int)

    # FM composite (simple average — can weight by business priority)
    rfm["fm_score"] = ((rfm["f_score"] + rfm["m_score"]) / 2).round().astype(int)

    rfm["rfm_score"]  = rfm["r_score"] * 100 + rfm["f_score"] * 10 + rfm["m_score"]
    rfm["segment"]    = rfm.apply(lambda row: get_segment(row["r_score"], row["fm_score"]), axis=1)
    rfm["monetary"]   = rfm["monetary"].round(2)
    rfm["snapshot_at"] = snapshot_date.isoformat()

    return rfm[[
        "customer_id", "last_order_date", "recency_days",
        "frequency", "monetary",
        "r_score", "f_score", "m_score", "rfm_score",
        "segment", "snapshot_at",
    ]]


# ── Orchestration ──────────────────────────────────────────────────────────

def run():
    s3     = s3_client()
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    print("[GOLD] Reading Silver orders...")
    df_orders = read_all_parquet(s3, "silver/orders_enriched/")

    # ── GMV Hourly ─────────────────────────────────────────────────────────
    print("[GOLD] Building GMV hourly table...")
    df_gmv = build_gmv_hourly(df_orders)
    if not df_gmv.empty:
        write_parquet(s3, df_gmv, f"gold/gmv_hourly/run={run_ts}/data.parquet")

    # ── RFM Segmentation ───────────────────────────────────────────────────
    print("[GOLD] Building Customer RFM table...")
    df_rfm = build_rfm(df_orders)
    if not df_rfm.empty:
        write_parquet(s3, df_rfm, f"gold/customer_rfm/run={run_ts}/data.parquet")

    print("[GOLD] Pipeline complete.")


if __name__ == "__main__":
    run()
