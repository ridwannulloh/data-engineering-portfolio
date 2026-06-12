"""
consumers/bronze_sink.py
─────────────────────────
Raw event consumer: reads from Kafka, writes partitioned JSON to MinIO
(Bronze layer of the Lakehouse).

Partitioning scheme:
    bronze/{topic}/year=YYYY/month=MM/day=DD/hour=HH/{uuid}.json

This is the immutable landing zone — no transformations applied.
All downstream Silver/Gold layers are derived from here.
"""

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.client import Config
from confluent_kafka import Consumer, KafkaException

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from config import (
    KAFKA_BOOTSTRAP_SERVERS, TOPICS,
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, BUCKET, S3_PATHS,
)

# ── S3 / MinIO client ──────────────────────────────────────────────────────

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )


# Map Kafka topics → Bronze folder names. Must stay aligned with the prefixes
# that pipelines/bronze_to_silver.py reads (e.g. bronze/inventory/, not
# bronze/inventory_updates/) and with the layout documented in the README.
BRONZE_FOLDER = {
    TOPICS["orders"]:      "orders",
    TOPICS["order_items"]: "order_items",
    TOPICS["inventory"]:   "inventory",
    TOPICS["clickstream"]: "clickstream",
}


def s3_key(topic: str, dt: datetime) -> str:
    short_topic = BRONZE_FOLDER.get(topic, topic.split(".")[-1])
    return (
        f"bronze/{short_topic}/"
        f"year={dt.year}/month={dt.month:02d}/"
        f"day={dt.day:02d}/hour={dt.hour:02d}/"
        f"{uuid.uuid4().hex}.json"
    )


# ── Batch writer ───────────────────────────────────────────────────────────

class BatchWriter:
    """
    Accumulates messages and flushes to S3 every N records or M seconds.
    This avoids one tiny S3 PUT per Kafka message (small-file problem).
    """

    def __init__(self, s3, topic: str, batch_size: int = 100, flush_interval: int = 30):
        self.s3             = s3
        self.topic          = topic
        self.batch_size     = batch_size
        self.flush_interval = flush_interval
        self.buffer: list[dict] = []
        self.last_flush     = datetime.now(timezone.utc)

    def add(self, record: dict):
        self.buffer.append(record)
        if len(self.buffer) >= self.batch_size or self._time_elapsed():
            self.flush()

    def _time_elapsed(self) -> bool:
        delta = (datetime.now(timezone.utc) - self.last_flush).total_seconds()
        return delta >= self.flush_interval

    def flush(self):
        if not self.buffer:
            return

        dt      = datetime.now(timezone.utc)
        key     = s3_key(self.topic, dt)
        payload = "\n".join(json.dumps(r) for r in self.buffer)  # NDJSON

        try:
            self.s3.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=payload.encode("utf-8"),
                ContentType="application/x-ndjson",
            )
            print(f"[BRONZE SINK] Flushed {len(self.buffer)} records → s3://{BUCKET}/{key}")
        except Exception as e:
            print(f"[BRONZE SINK] S3 write failed: {e}")

        self.buffer.clear()
        self.last_flush = datetime.now(timezone.utc)


# ── Consumer loop ──────────────────────────────────────────────────────────

def run(topics: list[str] = None, group_id: str = "bronze-sink-group"):
    if topics is None:
        topics = list(TOPICS.values())

    s3       = get_s3_client()
    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           group_id,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(topics)

    # One BatchWriter per topic
    writers = {t: BatchWriter(s3, t) for t in topics}

    print(f"[BRONZE SINK] Subscribed to: {topics}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                # Flush any stale buffers while idle
                for w in writers.values():
                    w.flush()
                continue

            if msg.error():
                raise KafkaException(msg.error())

            topic = msg.topic()
            try:
                record = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"[BRONZE SINK] Skipping malformed message: {e}")
                continue

            # Attach Kafka metadata for lineage
            record["_kafka_topic"]     = topic
            record["_kafka_partition"] = msg.partition()
            record["_kafka_offset"]    = msg.offset()
            record["_ingested_at"]     = datetime.now(timezone.utc).isoformat()

            writers[topic].add(record)

    except KeyboardInterrupt:
        print("\n[BRONZE SINK] Shutting down, flushing remaining records...")
    finally:
        for w in writers.values():
            w.flush()
        consumer.close()
        print("[BRONZE SINK] Done.")


if __name__ == "__main__":
    run()
