"""
Bronze Layer
------------
Reads raw Olist CSVs from data/raw/ and writes them to Delta tables
in data/bronze/, adding ingestion metadata columns.

Auto-downloads the Olist dataset from Kaggle using kagglehub if the
raw CSV files are not already present. Requires KAGGLE_API_TOKEN to
be set as an environment variable.

Usage:
    python -m src.ingestion.ingest_raw
"""
import shutil
from pathlib import Path

import kagglehub
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils.spark_session import get_spark

KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
RAW_DIR = Path("data/raw")
BRONZE_DIR = Path("data/bronze")

TABLES = [
    "olist_orders_dataset",
    "olist_order_items_dataset",
    "olist_customers_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "product_category_name_translation",
]


def download_raw_data() -> None:
    """Download the Olist dataset from Kaggle if any CSV is missing."""
    missing = [t for t in TABLES if not (RAW_DIR / f"{t}.csv").exists()]
    if not missing:
        print("[download] Raw data already present, skipping download.")
        return

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[download] Downloading '{KAGGLE_DATASET}' from Kaggle...")
    downloaded_path = Path(kagglehub.dataset_download(KAGGLE_DATASET))

    # Copy CSVs from the kagglehub cache directory into data/raw/
    for csv_file in downloaded_path.glob("*.csv"):
        target = RAW_DIR / csv_file.name
        if not target.exists():
            shutil.copy2(str(csv_file), str(target))

    print(f"[download] Raw data ready in: {RAW_DIR}")


def ingest_table(spark: SparkSession, name: str) -> None:
    src = str(RAW_DIR / f"{name}.csv")
    dst = str(BRONZE_DIR / name)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(src)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(src))
    )

    df.write.format("delta").mode("overwrite").save(dst)
    print(f"[bronze] {name}: {df.count():,} rows")


def main() -> None:
    download_raw_data()
    spark = get_spark("OlistBronze")
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        ingest_table(spark, table)
    spark.stop()
    print("Bronze layer complete.")


if __name__ == "__main__":
    main()
