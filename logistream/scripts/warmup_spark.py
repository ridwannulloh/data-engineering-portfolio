"""
Pre-download all Spark JARs during Docker image build.

Runs once at build time — subsequent container starts use the cached JARs
in /root/.ivy2/ instead of downloading from Maven at runtime.
"""

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("logistream-warmup")
    .master("local[1]")
    .config("spark.jars.ivy", "/root/.ivy2")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.ui.enabled", "false")
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"],
).getOrCreate()
print("[warmup] Spark JARs pre-downloaded successfully.")
spark.stop()
