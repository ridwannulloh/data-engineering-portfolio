"""
Airflow DAG
-----------
Orchestrates the full Olist pipeline daily at 02:00 UTC.

  bronze_ingestion → silver_transformation → gold_aggregation
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PIPELINE_DIR = "/opt/olist-etl-pipeline"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="olist_pipeline",
    default_args=default_args,
    description="Olist e-commerce Bronze → Silver → Gold pipeline",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "pyspark", "delta-lake"],
) as dag:

    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"cd {PIPELINE_DIR} && python -m src.ingestion.ingest_raw",
    )

    silver = BashOperator(
        task_id="silver_transformation",
        bash_command=f"cd {PIPELINE_DIR} && python -m src.transformation.clean_orders",
    )

    gold = BashOperator(
        task_id="gold_aggregation",
        bash_command=f"cd {PIPELINE_DIR} && python -m src.aggregation.customer_ltv",
    )

    bronze >> silver >> gold
