"""
Silver Layer
------------
Joins and cleans the Bronze tables into a single wide orders fact table.
Partitioned by purchase_year / purchase_month for efficient querying.

Usage:
    python -m src.transformation.clean_orders
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from src.utils.spark_session import get_spark

BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver"


def read(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.format("delta").load(f"{BRONZE_DIR}/{table}")


def add_order_dates(df: DataFrame) -> DataFrame:
    ts_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in ts_cols:
        df = df.withColumn(col, F.to_timestamp(col))

    return (
        df
        .filter(F.col("order_id").isNotNull())
        .withColumn(
            "delivery_days",
            F.datediff("order_delivered_customer_date", "order_purchase_timestamp"),
        )
        .withColumn(
            "is_late",
            F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"),
        )
        .withColumn("purchase_year",    F.year("order_purchase_timestamp"))
        .withColumn("purchase_month",   F.month("order_purchase_timestamp"))
        .withColumn("purchase_quarter", F.quarter("order_purchase_timestamp"))
    )


def build_orders_fact(spark: SparkSession) -> DataFrame:
    orders      = read(spark, "olist_orders_dataset")
    items       = read(spark, "olist_order_items_dataset")
    customers   = read(spark, "olist_customers_dataset")
    payments    = read(spark, "olist_order_payments_dataset")
    products    = read(spark, "olist_products_dataset")
    translation = read(spark, "product_category_name_translation")

    payments_agg = payments.groupBy("order_id").agg(
        F.sum("payment_value").cast(DoubleType()).alias("total_payment"),
        F.first("payment_installments").cast(IntegerType()).alias("payment_installments"),
        F.first("payment_type").alias("payment_type"),
    )

    items_agg = items.groupBy("order_id").agg(
        F.count("order_item_id").alias("item_count"),
        F.sum("price").alias("items_subtotal"),
        F.sum("freight_value").alias("freight_total"),
    )

    products_en = (
        products
        .join(translation, on="product_category_name", how="left")
        .select("product_id", F.col("product_category_name_english").alias("category"))
    )

    primary_category = (
        items
        .join(products_en, on="product_id", how="left")
        .groupBy("order_id")
        .agg(F.first("category").alias("primary_category"))
    )

    customers_slim = customers.select(
        "customer_id",
        "customer_unique_id",
        F.col("customer_city").alias("city"),
        F.col("customer_state").alias("state"),
    )

    return (
        add_order_dates(orders)
        .join(customers_slim,  on="customer_id", how="left")
        .join(payments_agg,    on="order_id",    how="left")
        .join(items_agg,       on="order_id",    how="left")
        .join(primary_category, on="order_id",   how="left")
    )


def main() -> None:
    spark = get_spark("OlistSilver")
    df = build_orders_fact(spark)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("purchase_year", "purchase_month")
        .save(f"{SILVER_DIR}/orders_fact")
    )
    print(f"[silver] orders_fact: {df.count():,} rows")
    spark.stop()
    print("Silver layer complete.")


if __name__ == "__main__":
    main()
