"""
View pipeline results using DuckDB.

Reads the gold-layer Delta tables (Parquet files) and prints
summary views for each analytical table.

Usage:
    python view_results.py                  # all tables
    python view_results.py customer_rfm     # single table
    python view_results.py monthly_revenue category_performance

Run from the olist-etl-pipeline/ directory (or inside the container):
    docker compose exec airflow-webserver bash -c \
        "cd /opt/olist-etl-pipeline && python view_results.py"
"""
import sys
from pathlib import Path

import duckdb

GOLD_DIR = Path("data/gold")

QUERIES = {
    "customer_rfm": {
        "title": "Customer RFM Segments",
        "summary": """
            SELECT
                segment,
                COUNT(*)                        AS customers,
                ROUND(AVG(recency_days), 1)     AS avg_recency_days,
                ROUND(AVG(frequency), 2)        AS avg_orders,
                ROUND(AVG(monetary), 2)         AS avg_spend_brl,
                ROUND(AVG(late_rate) * 100, 1)  AS late_delivery_pct
            FROM tbl
            GROUP BY segment
            ORDER BY avg_spend_brl DESC
        """,
        "sample": """
            SELECT
                customer_unique_id,
                segment,
                recency_days,
                frequency,
                ROUND(monetary, 2)  AS monetary_brl,
                r_score, f_score, m_score, rfm_score,
                state,
                top_category
            FROM tbl
            ORDER BY rfm_score DESC
            LIMIT 20
        """,
    },
    "monthly_revenue": {
        "title": "Monthly Revenue",
        "summary": """
            SELECT
                purchase_year,
                purchase_month,
                ROUND(SUM(revenue), 2)          AS total_revenue_brl,
                SUM(order_count)                AS total_orders,
                ROUND(AVG(avg_order_value), 2)  AS avg_order_value_brl,
                SUM(total_items_sold)           AS items_sold
            FROM tbl
            GROUP BY purchase_year, purchase_month
            ORDER BY purchase_year, purchase_month
        """,
        "sample": """
            SELECT
                purchase_year,
                purchase_month,
                state,
                ROUND(revenue, 2)               AS revenue_brl,
                order_count,
                ROUND(avg_order_value, 2)       AS avg_order_value_brl,
                ROUND(avg_delivery_days, 1)     AS avg_delivery_days
            FROM tbl
            ORDER BY revenue_brl DESC
            LIMIT 20
        """,
    },
    "category_performance": {
        "title": "Category Performance",
        "summary": """
            SELECT
                primary_category,
                ROUND(revenue, 2)               AS revenue_brl,
                order_count,
                ROUND(avg_order_value, 2)       AS avg_order_value_brl,
                ROUND(avg_delivery_days, 1)     AS avg_delivery_days,
                ROUND(late_rate * 100, 1)       AS late_delivery_pct
            FROM tbl
            ORDER BY revenue_brl DESC
        """,
        "sample": None,  # summary already has all rows
    },
}


def get_parquet_glob(table: str) -> str:
    path = GOLD_DIR / table
    if not path.exists():
        raise FileNotFoundError(
            f"Table not found: {path}\n"
            "Run the pipeline first (bronze → silver → gold)."
        )
    return str(path / "**/*.parquet")


def print_header(text: str) -> None:
    bar = "=" * 70
    print(f"\n{bar}")
    print(f"  {text}")
    print(f"{bar}")


def show_table(name: str) -> None:
    cfg = QUERIES[name]
    glob = get_parquet_glob(name)

    con = duckdb.connect()
    con.execute(f"CREATE VIEW tbl AS SELECT * FROM read_parquet('{glob}', hive_partitioning=true)")

    total_rows = con.execute("SELECT COUNT(*) FROM tbl").fetchone()[0]
    print_header(f"{cfg['title']}  —  {total_rows:,} total rows")

    print("\n[Summary]")
    result = con.execute(cfg["summary"]).df()
    print(result.to_string(index=False))

    if cfg["sample"]:
        print("\n[Top 20 rows]")
        result = con.execute(cfg["sample"]).df()
        print(result.to_string(index=False))

    con.close()


def main() -> None:
    requested = sys.argv[1:] if len(sys.argv) > 1 else list(QUERIES.keys())

    invalid = [t for t in requested if t not in QUERIES]
    if invalid:
        print(f"Unknown tables: {invalid}")
        print(f"Available: {list(QUERIES.keys())}")
        sys.exit(1)

    for name in requested:
        try:
            show_table(name)
        except FileNotFoundError as e:
            print(f"\n[SKIP] {name}: {e}")

    print()


if __name__ == "__main__":
    main()
