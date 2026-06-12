"""Single factory for Delta-enabled local SparkSessions.

Spark runs in local[*] mode inside the calling process (an Airflow task or a
pytest worker) — no cluster, no spark-submit. The hadoop-aws package is only
added when the lakehouse lives on MinIO/S3, so tests in 'local' storage mode
stay lightweight.
"""
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src import config

# Must match the Hadoop version bundled with pyspark 3.5.x, and must match
# what scripts/warm_spark_jars.py pre-resolves into the Docker image's Ivy
# cache — change both together.
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"


def get_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        # 50k rows doesn't need 200 shuffle partitions; 8 keeps tasks snappy.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
    )

    extra_packages = []
    if config.storage_backend() == "s3":
        extra_packages.append(HADOOP_AWS_PACKAGE)
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", config.minio_endpoint())
            .config("spark.hadoop.fs.s3a.access.key", config.minio_access_key())
            .config("spark.hadoop.fs.s3a.secret.key", config.minio_secret_key())
            # MinIO needs path-style addressing and plain HTTP locally.
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )

    return configure_spark_with_delta_pip(
        builder, extra_packages=extra_packages or None
    ).getOrCreate()
