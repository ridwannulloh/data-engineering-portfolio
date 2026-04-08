"""
Creates a persistent DuckDB database file (results.duckdb) with views
pointing to the gold, silver, and bronze Delta Lake Parquet files.

Open results.duckdb in DBeaver to browse and query all pipeline outputs.

Usage:
    python create_duckdb.py
"""
from pathlib import Path

import duckdb

DB_PATH = Path("results.duckdb")
GOLD_DIR = Path("data/gold")
SILVER_DIR = Path("data/silver")
BRONZE_DIR = Path("data/bronze")

VIEWS = {
    # Gold
    "gold_customer_rfm":         GOLD_DIR / "customer_rfm",
    "gold_monthly_revenue":      GOLD_DIR / "monthly_revenue",
    "gold_category_performance": GOLD_DIR / "category_performance",
    # Silver
    "silver_orders_fact":        SILVER_DIR / "orders_fact",
    # Bronze
    "bronze_orders":             BRONZE_DIR / "olist_orders_dataset",
    "bronze_order_items":        BRONZE_DIR / "olist_order_items_dataset",
    "bronze_customers":          BRONZE_DIR / "olist_customers_dataset",
    "bronze_products":           BRONZE_DIR / "olist_products_dataset",
    "bronze_sellers":            BRONZE_DIR / "olist_sellers_dataset",
    "bronze_payments":           BRONZE_DIR / "olist_order_payments_dataset",
    "bronze_reviews":            BRONZE_DIR / "olist_order_reviews_dataset",
    "bronze_category_translation": BRONZE_DIR / "product_category_name_translation",
}


def main() -> None:
    con = duckdb.connect(str(DB_PATH))

    created = []
    skipped = []

    for view_name, table_path in VIEWS.items():
        if not table_path.exists():
            skipped.append(str(table_path))
            continue

        glob = str(table_path.resolve() / "**/*.parquet").replace("\\", "/")
        con.execute(f"DROP VIEW IF EXISTS {view_name}")
        con.execute(
            f"CREATE VIEW {view_name} AS "
            f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true)"
        )
        row_count = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
        print(f"  [OK] {view_name:<35} {row_count:>10,} rows")
        created.append(view_name)

    con.close()

    print(f"\nDatabase saved to: {DB_PATH.resolve()}")
    print(f"Views created : {len(created)}")

    if skipped:
        print(f"Skipped (not found): {skipped}")
        print("Run the full pipeline first to generate missing tables.")

    print("\nConnect DBeaver to this file:")
    print("  Driver : DuckDB")
    print(f"  File   : {DB_PATH.resolve()}")


if __name__ == "__main__":
    main()
