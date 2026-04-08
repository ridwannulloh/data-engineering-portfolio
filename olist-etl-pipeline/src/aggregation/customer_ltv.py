"""
Gold Layer
----------
Produces three analytical tables from the Silver orders fact table:

  1. customer_rfm          — RFM segmentation (Champions, At Risk, etc.)
  2. monthly_revenue       — GMV and order metrics by state and month
  3. category_performance  — Revenue and delivery metrics by product category

Usage:
    python -m src.aggregation.customer_ltv
"""
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.utils.spark_session import get_spark

SILVER_DIR = "data/silver"
GOLD_DIR   = "data/gold"


def customer_rfm(df: DataFrame) -> DataFrame:
    max_date = df.agg(F.max("order_purchase_timestamp")).collect()[0][0]

    base = (
        df
        .filter(F.col("order_status") == "delivered")
        .groupBy("customer_unique_id")
        .agg(
            F.datediff(F.lit(max_date), F.max("order_purchase_timestamp")).alias("recency_days"),
            F.countDistinct("order_id").alias("frequency"),
            F.sum("total_payment").alias("monetary"),
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.avg(F.col("is_late").cast(IntegerType())).alias("late_rate"),
            F.first("state").alias("state"),
            F.first("primary_category").alias("top_category"),
        )
    )

    w_recency   = Window.orderBy(F.desc("recency_days"))
    w_frequency = Window.orderBy("frequency")
    w_monetary  = Window.orderBy("monetary")

    scored = (
        base
        .withColumn("r_score", F.ntile(5).over(w_recency))
        .withColumn("f_score", F.ntile(5).over(w_frequency))
        .withColumn("m_score", F.ntile(5).over(w_monetary))
        .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
    )

    return scored.withColumn(
        "segment",
        F.when(F.col("rfm_score") >= 13, "Champions")
         .when(F.col("rfm_score") >= 10, "Loyal Customers")
         .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2), "New Customers")
         .when((F.col("r_score") <= 2) & (F.col("f_score") >= 4), "At Risk")
         .when(F.col("rfm_score") <= 5, "Lost")
         .otherwise("Potential Loyalists"),
    )


def monthly_revenue(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("order_status") == "delivered")
        .groupBy("purchase_year", "purchase_month", "state")
        .agg(
            F.sum("total_payment").alias("revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("total_payment").alias("avg_order_value"),
            F.sum("item_count").alias("total_items_sold"),
            F.avg("delivery_days").alias("avg_delivery_days"),
        )
    )


def category_performance(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("order_status") == "delivered")
        .groupBy("primary_category")
        .agg(
            F.sum("total_payment").alias("revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("total_payment").alias("avg_order_value"),
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.avg(F.col("is_late").cast(IntegerType())).alias("late_rate"),
        )
    )


def save(df: DataFrame, name: str) -> None:
    path = f"{GOLD_DIR}/{name}"
    df.cache()
    row_count = df.count()
    df.write.format("delta").mode("overwrite").save(path)
    df.unpersist()
    print(f"[gold] {name}: {row_count:,} rows")


def main() -> None:
    spark = get_spark("OlistGold")
    os.makedirs(GOLD_DIR, exist_ok=True)

    orders = spark.read.format("delta").load(f"{SILVER_DIR}/orders_fact")

    save(customer_rfm(orders),        "customer_rfm")
    save(monthly_revenue(orders),     "monthly_revenue")
    save(category_performance(orders), "category_performance")

    spark.stop()
    print("Gold layer complete.")


if __name__ == "__main__":
    main()
