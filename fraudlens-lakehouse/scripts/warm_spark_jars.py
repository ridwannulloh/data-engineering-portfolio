"""Build-time helper (see Dockerfile): start a throwaway SparkSession so
Ivy resolves the Delta Lake + hadoop-aws jars into the image's cache. The
package list MUST match src/spark_session.py, otherwise the first DAG run
re-downloads from Maven and blows the two-minute demo budget.
"""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"

builder = (
    SparkSession.builder.master("local[1]")
    .appName("warm_spark_jars")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)
spark = configure_spark_with_delta_pip(
    builder, extra_packages=[HADOOP_AWS_PACKAGE]
).getOrCreate()
spark.range(1).count()
spark.stop()
print("Spark jar cache warmed: Delta Lake + hadoop-aws resolved.")
