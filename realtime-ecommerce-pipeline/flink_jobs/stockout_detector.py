"""
flink_jobs/stockout_detector.py
────────────────────────────────
Detects products at risk of stockout using a sliding window over
inventory events from Kafka.

Logic:
  - Consume inventory_updates topic
  - Sliding window: 15-min window, slide every 5 min
  - Flag products where:
      • quantity_after < 20  (low stock threshold)
      • OR net delta over window is negative AND rate > 5 units/min
  - Emit alerts downstream (print sink — swap for Kafka/Slack/PagerDuty)

This mirrors the kind of real-time operational alerting pattern
that e-commerce and retail clients pay engineers to build.
"""

import json
from datetime import datetime
from typing import Iterator, Tuple

from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.window import SlidingEventTimeWindows, Time
from pyflink.datastream.functions import ProcessWindowFunction

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS

# ── Constants ──────────────────────────────────────────────────────────────
LOW_STOCK_THRESHOLD = 20     # units
MIN_DEPLETION_RATE  = 5.0    # units per minute to trigger rate-based alert


# ── Window function ────────────────────────────────────────────────────────

class StockoutWindowFunction(ProcessWindowFunction):
    """
    Per-product sliding window: compute net delta, depletion rate, and
    latest stock level, then emit an alert record if thresholds are breached.
    """

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        events,
    ) -> Iterator[str]:
        event_list = list(events)
        if not event_list:
            return

        # Parse raw JSON strings from upstream map step
        records = [json.loads(e) if isinstance(e, str) else e for e in event_list]

        # Latest snapshot
        latest          = max(records, key=lambda r: r.get("recorded_at", ""))
        current_qty     = latest.get("quantity_after", 0)
        product_id      = latest.get("product_id", key)
        warehouse_id    = latest.get("warehouse_id", "unknown")
        category        = latest.get("category", "unknown")

        # Net depletion over window
        net_delta       = sum(r.get("delta_quantity", 0) for r in records)
        sale_events     = [r for r in records if r.get("event_type") == "sale"]
        units_sold      = abs(sum(r.get("delta_quantity", 0) for r in sale_events))

        # Window duration in minutes
        window          = context.window()
        window_minutes  = (window.end - window.start) / 60_000  # ms → min
        depletion_rate  = units_sold / window_minutes if window_minutes > 0 else 0

        # ── Alert conditions ───────────────────────────────────────────────
        is_low_stock     = current_qty < LOW_STOCK_THRESHOLD
        is_high_velocity = depletion_rate >= MIN_DEPLETION_RATE and net_delta < 0

        if is_low_stock or is_high_velocity:
            alert_level = "CRITICAL" if current_qty == 0 else ("HIGH" if is_low_stock else "MEDIUM")
            alert = {
                "alert_type":      "STOCKOUT_RISK",
                "alert_level":     alert_level,
                "product_id":      product_id,
                "warehouse_id":    warehouse_id,
                "category":        category,
                "current_qty":     current_qty,
                "net_delta":       net_delta,
                "units_sold_window": units_sold,
                "depletion_rate_per_min": round(depletion_rate, 2),
                "is_low_stock":    is_low_stock,
                "is_high_velocity": is_high_velocity,
                "window_start":    datetime.utcfromtimestamp(window.start / 1000).isoformat(),
                "window_end":      datetime.utcfromtimestamp(window.end   / 1000).isoformat(),
                "alerted_at":      datetime.utcnow().isoformat(),
            }
            yield json.dumps(alert)


# ── Main ──────────────────────────────────────────────────────────────────

def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.set_parallelism(2)

    # ── Source ─────────────────────────────────────────────────────────────
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(TOPICS["inventory"])
        .set_group_id("flink-stockout-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(
            lambda raw, _: int(
                datetime.fromisoformat(
                    json.loads(raw).get("recorded_at", datetime.utcnow().isoformat())
                ).timestamp() * 1000
            )
        )
    )

    stream = env.from_source(source, watermark_strategy, "Kafka Inventory Source")

    # ── Sliding window per product ─────────────────────────────────────────
    alert_stream = (
        stream
        .key_by(lambda raw: json.loads(raw).get("product_id", "unknown"))
        .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(5)))
        .process(StockoutWindowFunction())
    )

    alert_stream.print()

    env.execute("Ecommerce Stockout Risk Detector")


if __name__ == "__main__":
    build_pipeline()
