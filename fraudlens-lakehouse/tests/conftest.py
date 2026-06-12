"""Shared fixtures. Tests need no Docker and no MinIO: Spark runs in
local[1] mode and the lakehouse is repointed at a pytest tmp directory via
STORAGE_BACKEND=local (config reads env at call time precisely for this).
"""
import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("fraudlens_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture()
def lakehouse_env(tmp_path, monkeypatch):
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("LOCAL_LAKEHOUSE_DIR", str(tmp_path / "lakehouse"))
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("DATA_NUM_ROWS", "3000")
    return tmp_path
