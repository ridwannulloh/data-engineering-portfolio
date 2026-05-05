"""
LogiStream — Spark Structured Streaming job.

Reads from three Kafka topics, applies Medallion transformations,
and writes to Delta Lake.

Run:
    spark-submit \
        --packages io.delta:delta-spark_2.12:3.1.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        spark/streaming_job.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

from config.settings import settings
from spark.transforms import (
    SHIPMENT_SCHEMA, CARRIER_SCHEMA, WAREHOUSE_SCHEMA,
    parse_shipment_events, clean_and_enrich,
    carrier_delay_agg, active_alerts,
)
from spark.delta_writer import delta_sink


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(settings.spark_app_name)
        .master(settings.spark_master)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.ivy", "/root/.ivy2")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        # Adaptive Query Execution — improves join & shuffle performance
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    )
    # Pass Kafka connector via extra_packages — configure_spark_with_delta_pip
    # overwrites spark.jars.packages internally, so this is the correct way
    # to load both Delta and Kafka JARs together.
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"],
    ).getOrCreate()


def kafka_source(spark: SparkSession, topic: str):    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 1000)
        .option("failOnDataLoss", "false")
        .load()
    )


def run():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("[LogiStream] Spark session ready.")

    # ── 1. Read raw Kafka streams ────────────────────────────────────────────
    raw_shipments  = kafka_source(spark, settings.kafka_topic_shipments)
    raw_carriers   = kafka_source(spark, settings.kafka_topic_carriers)
    raw_warehouses = kafka_source(spark, settings.kafka_topic_warehouses)

    # ── 2. Bronze: parse to typed schema ────────────────────────────────────
    bronze_shipments = parse_shipment_events(raw_shipments)

    bronze_carriers = (
        raw_carriers
        .select(
            F.from_json(F.col("value").cast("string"), CARRIER_SCHEMA).alias("d"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("d.*", "kafka_ts")
        .withColumn("ingested_at", F.current_timestamp())
    )

    bronze_warehouses = (
        raw_warehouses
        .select(
            F.from_json(F.col("value").cast("string"), WAREHOUSE_SCHEMA).alias("d"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("d.*", "kafka_ts")
        .withColumn("ingested_at", F.current_timestamp())
    )

    # ── 3. Silver: clean + enrich ────────────────────────────────────────────
    silver_shipments = clean_and_enrich(bronze_shipments)

    # ── 4. Gold: windowed aggregations + alerts ──────────────────────────────
    gold_carrier_kpis = carrier_delay_agg(silver_shipments, "5 minutes")
    gold_alerts       = active_alerts(silver_shipments)

    # ── 5. Write to Delta Lake ───────────────────────────────────────────────
    cp = settings.spark_checkpoint_path

    queries = [
        delta_sink(bronze_shipments,  f"{settings.delta_bronze_path}/shipments",
                   f"{cp}/bronze/shipments",   trigger_secs=60),
        delta_sink(bronze_carriers,   f"{settings.delta_bronze_path}/carriers",
                   f"{cp}/bronze/carriers",    trigger_secs=60),
        delta_sink(bronze_warehouses, f"{settings.delta_bronze_path}/warehouses",
                   f"{cp}/bronze/warehouses",  trigger_secs=60),

        delta_sink(silver_shipments,  f"{settings.delta_silver_path}/shipments",
                   f"{cp}/silver/shipments",   trigger_secs=60),

        delta_sink(gold_carrier_kpis, f"{settings.delta_gold_path}/carrier_kpis",
                   f"{cp}/gold/carrier_kpis",
                   output_mode="append",       trigger_secs=60),

        delta_sink(gold_alerts,       f"{settings.delta_gold_path}/alerts",
                   f"{cp}/gold/alerts",        trigger_secs=60),
    ]

    print(f"[LogiStream] {len(queries)} streaming queries active. Awaiting termination.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
