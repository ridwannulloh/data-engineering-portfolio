"""FraudLens lakehouse DAG: raw CSV → Bronze → Silver → Gold, Delta on MinIO.

Scheduled @once and unpaused so the pipeline runs itself the moment the stack
starts — open the Airflow UI and the demo is already in motion. Re-run any
time with a manual trigger.

Heavy imports (pyspark) happen inside the task callables, not at module
level, so the scheduler's DAG-parse loop stays fast.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def _generate_raw_data():
    from src.generate_data import run

    run()


def _bronze_ingest():
    from src.bronze.ingest_transactions import run

    run()


def _silver_clean():
    from src.silver.clean_transactions import run

    run()


def _gold_metrics():
    from src.gold.fraud_metrics import run

    print(f"Detection metrics: {run()}")


with DAG(
    dag_id="fraud_lakehouse",
    description="PaySim-style fraud analytics: Bronze → Silver → Gold on Delta Lake",
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False,
    # Retries cover the first seconds after `docker compose up`, when MinIO
    # may accept connections a beat later than the scheduler starts.
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["finance", "fraud-detection", "pyspark", "delta-lake", "medallion"],
) as dag:
    generate_raw_data = PythonOperator(
        task_id="generate_raw_data", python_callable=_generate_raw_data
    )
    bronze_ingest = PythonOperator(
        task_id="bronze_ingest", python_callable=_bronze_ingest
    )
    silver_clean = PythonOperator(task_id="silver_clean", python_callable=_silver_clean)
    gold_metrics = PythonOperator(task_id="gold_metrics", python_callable=_gold_metrics)

    generate_raw_data >> bronze_ingest >> silver_clean >> gold_metrics
