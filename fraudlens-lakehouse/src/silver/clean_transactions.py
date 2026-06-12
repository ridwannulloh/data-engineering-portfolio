"""Silver: clean, conform and enrich Bronze transactions.

Three responsibilities, in order:
  1. Conform  — PaySim's mixedCase columns become snake_case with clear names.
  2. Clean    — dedupe on the natural key, drop non-positive amounts and
                rows missing account identifiers.
  3. Enrich   — derive the behavioural features the Gold fraud rules consume
                (full balance drain, stale destination balance, time-of-day).

Feature engineering lives here, NOT in Gold: Silver is the single typed,
documented contract that every downstream consumer (Gold tables, ad-hoc
analysts, future ML jobs) reads from.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src import config
from src.spark_session import get_spark

COLUMN_RENAMES = {
    "type": "txn_type",
    "nameOrig": "orig_account",
    "oldbalanceOrg": "orig_balance_before",
    "newbalanceOrig": "orig_balance_after",
    "nameDest": "dest_account",
    "oldbalanceDest": "dest_balance_before",
    "newbalanceDest": "dest_balance_after",
    "isFraud": "is_fraud",
    "isFlaggedFraud": "is_flagged_fraud",
}

# In PaySim (and its real-world inspiration) fraud only occurs through
# channels that move cash out of the system.
HIGH_RISK_TYPES = ["TRANSFER", "CASH_OUT"]

NATURAL_KEY = ["step", "txn_type", "amount", "orig_account", "dest_account"]


def run(spark: SparkSession | None = None) -> int:
    """Airflow task entrypoint. Returns the Silver row count."""
    owns_spark = spark is None
    if owns_spark:
        spark = get_spark("silver_clean_transactions")
    try:
        df = spark.read.format("delta").load(config.bronze_path())
        for old_name, new_name in COLUMN_RENAMES.items():
            df = df.withColumnRenamed(old_name, new_name)

        cleaned = (
            df.dropDuplicates(NATURAL_KEY)
            .filter(F.col("amount") > 0)
            .filter(
                F.col("orig_account").isNotNull() & F.col("dest_account").isNotNull()
            )
        )

        enriched = (
            cleaned.withColumn("hour_of_day", F.col("step") % 24)
            .withColumn("sim_day", ((F.col("step") - 1) / 24).cast("int") + 1)
            .withColumn("is_merchant_dest", F.col("dest_account").startswith("M"))
            .withColumn("is_high_risk_type", F.col("txn_type").isin(HIGH_RISK_TYPES))
            # Account emptied in a single transaction — classic takeover move.
            .withColumn(
                "full_balance_drain",
                (F.col("orig_balance_before") > 0)
                & (F.abs(F.col("amount") - F.col("orig_balance_before")) < 0.01)
                & (F.col("orig_balance_after") == 0),
            )
            # Money moved out, but the destination balance never increased:
            # the strongest mule-account signal in this dataset.
            .withColumn(
                "dest_balance_stale",
                F.col("is_high_risk_type")
                & ~F.col("is_merchant_dest")
                & (F.col("dest_balance_after") <= F.col("dest_balance_before")),
            )
            .withColumn(
                "amount_to_balance_ratio",
                F.when(
                    F.col("orig_balance_before") > 0,
                    F.round(F.col("amount") / F.col("orig_balance_before"), 4),
                ),
            )
        )

        (
            enriched.write.format("delta")
            .mode("overwrite")
            .partitionBy("txn_type")
            .save(config.silver_path())
        )

        count = spark.read.format("delta").load(config.silver_path()).count()
        print(f"Silver: wrote {count:,} rows to {config.silver_path()}")
        return count
    finally:
        if owns_spark:
            spark.stop()


if __name__ == "__main__":
    run()
