"""
pipelines/airflow_dag.py
─────────────────────────
Airflow DAG: orchestrates the batch pipeline layers.

Schedule: every 30 minutes.

Task graph:
  start
    └─► bronze_to_silver_orders
    └─► bronze_to_silver_inventory
          └─► silver_to_gold_gmv
          └─► silver_to_gold_rfm
                └─► end

Note: Producers (Kafka) and Flink jobs run as long-lived services
      managed by Docker Compose — they are NOT Airflow tasks.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Import pipeline functions
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipelines.bronze_to_silver import run as bronze_to_silver_run
from pipelines.silver_to_gold   import run as silver_to_gold_run


# ── Default args ──────────────────────────────────────────────────────────

default_args = {
    "owner":            "ridwan",
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

# ── DAG definition ────────────────────────────────────────────────────────

with DAG(
    dag_id="ecommerce_realtime_pipeline",
    description="Real-time e-commerce data pipeline: Bronze → Silver → Gold",
    schedule_interval="*/30 * * * *",   # every 30 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "streaming", "medallion"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # ── Bronze → Silver ────────────────────────────────────────────────────
    bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=bronze_to_silver_run,
        doc_md="""
        Reads raw NDJSON from the Bronze S3 layer (orders + inventory).
        Applies data quality checks, rejects malformed records to quarantine,
        and writes cleaned Parquet files to Silver.
        """,
    )

    # ── Silver → Gold ──────────────────────────────────────────────────────
    silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=silver_to_gold_run,
        doc_md="""
        Reads Silver Parquet and computes:
          - Hourly GMV by channel
          - Customer RFM segmentation (quintile scoring)
        Writes results to Gold layer as Snappy-compressed Parquet.
        """,
    )

    # ── Dependencies ───────────────────────────────────────────────────────
    start >> bronze_to_silver >> silver_to_gold >> end
