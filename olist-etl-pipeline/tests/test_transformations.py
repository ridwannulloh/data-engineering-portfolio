"""
Unit tests for the Silver transformation logic.

Run with:
    pytest tests/ -v
"""
import pytest
from pyspark.sql import SparkSession

from src.transformation.clean_orders import add_order_dates

COLS = [
    "order_id", "customer_id", "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("OlistTests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def make_df(spark, rows):
    return spark.createDataFrame(rows, COLS)


def test_delivery_days(spark):
    df = make_df(spark, [("o1", "c1", "delivered",
        "2018-01-01 10:00:00", "2018-01-01 11:00:00",
        "2018-01-03 10:00:00", "2018-01-06 10:00:00",
        "2018-01-08 10:00:00")])
    row = add_order_dates(df).collect()[0]
    assert row["delivery_days"] == 5


def test_is_late_false(spark):
    df = make_df(spark, [("o1", "c1", "delivered",
        "2018-01-01 10:00:00", None, None,
        "2018-01-06 10:00:00", "2018-01-08 10:00:00")])
    row = add_order_dates(df).collect()[0]
    assert row["is_late"] is False


def test_is_late_true(spark):
    df = make_df(spark, [("o1", "c1", "delivered",
        "2018-01-01 10:00:00", None, None,
        "2018-01-10 10:00:00", "2018-01-08 10:00:00")])
    row = add_order_dates(df).collect()[0]
    assert row["is_late"] is True


def test_null_order_id_dropped(spark):
    df = make_df(spark, [
        ("o1", "c1", "delivered", None, None, None, None, None),
        (None, "c2", "shipped",   None, None, None, None, None),
    ])
    assert add_order_dates(df).count() == 1


def test_purchase_year_month(spark):
    df = make_df(spark, [("o1", "c1", "delivered",
        "2018-06-15 10:00:00", None, None, None, None)])
    row = add_order_dates(df).collect()[0]
    assert row["purchase_year"] == 2018
    assert row["purchase_month"] == 6
