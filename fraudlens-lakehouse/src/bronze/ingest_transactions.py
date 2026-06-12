"""Bronze: land the raw transactions CSV in Delta, byte-faithful to source.

Bronze keeps PaySim's original column names and adds nothing but ingestion
metadata — so any downstream bug can always be replayed from here. Type
enforcement happens via an explicit schema (never inferSchema in a pipeline:
inference is slow and can silently change types between runs).
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src import config
from src.spark_session import get_spark

RAW_SCHEMA = StructType(
    [
        StructField("step", IntegerType()),
        StructField("type", StringType()),
        StructField("amount", DoubleType()),
        StructField("nameOrig", StringType()),
        StructField("oldbalanceOrg", DoubleType()),
        StructField("newbalanceOrig", DoubleType()),
        StructField("nameDest", StringType()),
        StructField("oldbalanceDest", DoubleType()),
        StructField("newbalanceDest", DoubleType()),
        StructField("isFraud", IntegerType()),
        StructField("isFlaggedFraud", IntegerType()),
    ]
)


def run(spark: SparkSession | None = None) -> int:
    """Airflow task entrypoint. Returns the Bronze row count."""
    owns_spark = spark is None
    if owns_spark:
        spark = get_spark("bronze_ingest_transactions")
    try:
        raw = spark.read.csv(
            config.raw_transactions_path(), header=True, schema=RAW_SCHEMA
        )
        bronze = raw.withColumn("ingested_at", F.current_timestamp()).withColumn(
            "source_file", F.input_file_name()
        )
        # Overwrite keeps the demo idempotent: re-triggering the DAG always
        # produces the same lakehouse state. Delta retains every version
        # anyway (time travel), so nothing is actually lost.
        bronze.write.format("delta").mode("overwrite").save(config.bronze_path())

        count = spark.read.format("delta").load(config.bronze_path()).count()
        print(f"Bronze: wrote {count:,} rows to {config.bronze_path()}")
        return count
    finally:
        if owns_spark:
            spark.stop()


if __name__ == "__main__":
    run()
