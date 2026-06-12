"""
flink_jobs/gmv_aggregation.py
─────────────────────────────
Real-time GMV (Gross Merchandise Value) computation using PyFlink.

Processing logic:
  - Consume order events from Kafka
  - Filter for 'order_placed' events only
  - Tumbling window: 1-hour GMV per channel (web / mobile / partner_api)
  - Output enriched window result to MinIO (Silver → Gold layer)

Run:
    python flink_jobs/gmv_aggregation.py
"""

import json
import os
from datetime import datetime

from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import (
    ProcessWindowFunction,
    AggregateFunction,
    RuntimeContext,
)

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS


# ── Aggregate: running GMV + order count ──────────────────────────────────

class GMVAccumulator:
    def __init__(self):
        self.total_gmv    = 0.0
        self.order_count  = 0
        self.channel      = ""


class GMVAggregateFunction(AggregateFunction):
    def create_accumulator(self):
        return GMVAccumulator()

    def add(self, value: dict, acc: GMVAccumulator):
        acc.total_gmv   += value.get("total_amount", 0.0)
        acc.order_count += 1
        acc.channel      = value.get("channel", "unknown")
        return acc

    def get_result(self, acc: GMVAccumulator):
        return acc

    def merge(self, a: GMVAccumulator, b: GMVAccumulator):
        a.total_gmv   += b.total_gmv
        a.order_count += b.order_count
        return a


# ── Window: emit final record with window boundaries ──────────────────────

class GMVWindowFunction(ProcessWindowFunction):
    def process(self, key, context, accumulators):
        acc    = list(accumulators)[0]
        window = context.window()

        result = {
            "channel":      key,
            "window_start": datetime.utcfromtimestamp(window.start / 1000).isoformat(),
            "window_end":   datetime.utcfromtimestamp(window.end   / 1000).isoformat(),
            "total_gmv":    round(acc.total_gmv, 2),
            "order_count":  acc.order_count,
            "avg_order_value": round(acc.total_gmv / acc.order_count, 2) if acc.order_count else 0,
            "processed_at": datetime.utcnow().isoformat(),
        }
        yield json.dumps(result)


# ── Parse + filter helper ─────────────────────────────────────────────────

def parse_and_filter(raw: str):
    """Return (event_dict, channel) for order_placed events, else None."""
    try:
        evt = json.loads(raw)
        if evt.get("event_type") == "order_placed":
            return evt
    except json.JSONDecodeError:
        pass
    return None


# ── Main ──────────────────────────────────────────────────────────────────

def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.set_parallelism(2)

    # ── Source: Kafka ──────────────────────────────────────────────────────
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(TOPICS["orders"])
        .set_group_id("flink-gmv-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Bounded watermark: allow 10-second late arrivals
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(
            lambda event, _: int(
                datetime.fromisoformat(
                    json.loads(event).get("created_at", datetime.utcnow().isoformat())
                ).timestamp() * 1000
            )
        )
    )

    stream = env.from_source(source, watermark_strategy, "Kafka Order Source")

    # ── Transformations ────────────────────────────────────────────────────
    gmv_stream = (
        stream
        .map(parse_and_filter)
        .filter(lambda x: x is not None)
        .key_by(lambda evt: evt.get("channel", "unknown"))
        .window(TumblingEventTimeWindows.of(Time.hours(1)))
        .aggregate(GMVAggregateFunction(), GMVWindowFunction())
    )

    # ── Sink: print (replace with S3/Kafka sink for production) ──────────
    gmv_stream.print()

    env.execute("Ecommerce Real-Time GMV Aggregation")


if __name__ == "__main__":
    build_pipeline()
