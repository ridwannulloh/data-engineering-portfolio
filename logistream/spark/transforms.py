"""
PySpark transformation logic for the LogiStream pipeline.

Separated from the streaming job for testability — all functions
accept and return DataFrames or Column expressions, no SparkSession dependency.
"""

from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType, BooleanType,
)

# ── Schemas ─────────────────────────────────────────────────────────────────

SHIPMENT_SCHEMA = StructType([
    StructField("shipment_id",   StringType(),    False),
    StructField("event_type",    StringType(),    False),
    StructField("carrier_code",  StringType(),    False),
    StructField("service_type",  StringType(),    False),
    StructField("origin_wh",     StringType(),    True),
    StructField("dest_city",     StringType(),    True),
    StructField("dest_country",  StringType(),    True),
    StructField("weight_kg",     DoubleType(),    True),
    StructField("promised_eta",  StringType(),    True),
    StructField("event_ts",      StringType(),    False),
    StructField("latitude",      DoubleType(),    True),
    StructField("longitude",     DoubleType(),    True),
    StructField("delay_minutes", IntegerType(),   True),
    StructField("notes",         StringType(),    True),
])

CARRIER_SCHEMA = StructType([
    StructField("carrier_code",      StringType(),  False),
    StructField("region",            StringType(),  True),
    StructField("active_shipments",  IntegerType(), True),
    StructField("on_time_rate",      DoubleType(),  True),
    StructField("avg_delay_minutes", DoubleType(),  True),
    StructField("report_ts",         StringType(),  False),
])

WAREHOUSE_SCHEMA = StructType([
    StructField("warehouse_id", StringType(),  False),
    StructField("operation",    StringType(),  True),
    StructField("shipment_id",  StringType(),  True),
    StructField("operator_id",  StringType(),  True),
    StructField("dock_door",    IntegerType(), True),
    StructField("op_ts",        StringType(),  False),
])

# ── SLA configuration ────────────────────────────────────────────────────────

SLA_HOURS = {
    "STANDARD":  48,
    "EXPRESS":   24,
    "OVERNIGHT": 12,
}


# ── Bronze: raw ingest ───────────────────────────────────────────────────────

def parse_shipment_events(raw_df: DataFrame) -> DataFrame:
    """
    Parse raw Kafka JSON bytes into a typed Bronze DataFrame.
    Enforces StructType schema — malformed rows land in `_corrupt_record`.
    """
    return (
        raw_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                SHIPMENT_SCHEMA,
                {"mode": "PERMISSIVE"},
            ).alias("data"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("partition"),
            F.col("offset"),
        )
        .select("data.*", "kafka_ts", "partition", "offset")
        .withColumn("ingested_at", F.current_timestamp())
    )


# ── Silver: cleaning + enrichment ────────────────────────────────────────────

def clean_and_enrich(bronze_df: DataFrame) -> DataFrame:
    """
    Apply Silver-layer transformations:
    - Parse timestamps to TimestampType
    - Classify SLA breach status
    - Add geographic region bucket
    - Deduplicate within micro-batch on (shipment_id, event_ts)
    """
    sla_hours_expr = (
        F.when(F.col("service_type") == "OVERNIGHT", SLA_HOURS["OVERNIGHT"])
         .when(F.col("service_type") == "EXPRESS",   SLA_HOURS["EXPRESS"])
         .otherwise(SLA_HOURS["STANDARD"])
    )

    return (
        bronze_df
        .filter(F.col("shipment_id").isNotNull())
        .withColumn("event_ts",    F.to_timestamp("event_ts"))
        .withColumn("promised_eta", F.to_timestamp("promised_eta"))
        # SLA threshold in minutes
        .withColumn("sla_minutes", sla_hours_expr * 60)
        # Is this shipment already breaching SLA?
        .withColumn(
            "sla_breached",
            F.col("delay_minutes") > F.col("sla_minutes"),
        )
        # Severity label
        .withColumn(
            "delay_severity",
            F.when(F.col("delay_minutes") == 0,               "NONE")
             .when(F.col("delay_minutes") < 60,               "LOW")
             .when(F.col("delay_minutes") < 240,              "MEDIUM")
             .otherwise("HIGH"),
        )
        # Broad US geographic bucket from longitude
        .withColumn(
            "geo_region",
            F.when(F.col("longitude") < -115, "West")
             .when(F.col("longitude") < -100, "Mountain")
             .when(F.col("longitude") < -85,  "Central")
             .otherwise("East"),
        )
        # Dedup within micro-batch
        .dropDuplicates(["shipment_id", "event_ts"])
        .withColumn("processed_at", F.current_timestamp())
    )


# ── Gold: aggregations ────────────────────────────────────────────────────────

def carrier_delay_agg(silver_df: DataFrame, window_duration: str = "5 minutes") -> DataFrame:
    """
    5-minute tumbling window: carrier-level delay KPIs.

    Columns: carrier_code, window_start, window_end,
             total_events, delayed_events, avg_delay_minutes,
             sla_breach_rate, p95_delay_minutes
    """
    return (
        silver_df
        .withWatermark("event_ts", "10 minutes")
        .groupBy(
            F.window("event_ts", window_duration),
            F.col("carrier_code"),
        )
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("delay_minutes") > 0, 1).otherwise(0))
             .alias("delayed_events"),
            F.round(F.avg("delay_minutes"), 1).alias("avg_delay_minutes"),
            F.round(
                F.sum(F.col("sla_breached").cast("int")) / F.count("*"), 3
            ).alias("sla_breach_rate"),
            F.round(F.percentile_approx("delay_minutes", 0.95), 0)
             .alias("p95_delay_minutes"),
        )
        .select(
            F.col("carrier_code"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_events",
            "delayed_events",
            "avg_delay_minutes",
            "sla_breach_rate",
            "p95_delay_minutes",
        )
    )


def active_alerts(silver_df: DataFrame) -> DataFrame:
    """
    Identify shipments in EXCEPTION or HIGH-severity delay — surface as alerts.
    """
    return (
        silver_df
        .filter(
            (F.col("event_type") == "EXCEPTION")
            | (F.col("delay_severity") == "HIGH")
            | F.col("sla_breached")
        )
        .select(
            "shipment_id",
            "event_type",
            "carrier_code",
            "service_type",
            "delay_minutes",
            "delay_severity",
            "sla_breached",
            "origin_wh",
            "dest_city",
            "event_ts",
            "notes",
        )
        .withColumn("alert_generated_at", F.current_timestamp())
    )
