"""Pretty-print all Gold tables — the one-command demo screenshot.

Inside Docker:
    docker compose exec airflow-scheduler python /opt/airflow/scripts/show_results.py
Locally (STORAGE_BACKEND=local after a local run):
    python scripts/show_results.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import config  # noqa: E402
from src.spark_session import get_spark  # noqa: E402

GOLD_TABLES = [
    "fraud_by_type",
    "hourly_fraud_pattern",
    "high_risk_accounts",
    "detection_performance",
]


def main() -> None:
    spark = get_spark("show_gold_results")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        for table_name in GOLD_TABLES:
            path = config.gold_path(table_name)
            print(f"\n{'=' * 60}\n  {table_name}  ({path})\n{'=' * 60}")
            spark.read.format("delta").load(path).show(24, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
