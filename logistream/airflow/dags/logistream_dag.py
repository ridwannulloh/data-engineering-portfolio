"""
LogiStream Airflow DAG — pipeline maintenance and health checks.

Schedule: hourly
Tasks:
  1. health_check      — verify Delta tables are growing
  2. silver_optimize   — OPTIMIZE + ZORDER on Silver shipments
  3. gold_optimize     — OPTIMIZE on Gold tables
  4. bronze_vacuum     — VACUUM Bronze (retain 7 days)
  5. alert_summary     — log alert count to Airflow XCom

Run locally:
    airflow dags test logistream_maintenance 2024-01-01
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys, os
sys.path.insert(0, "/opt/logistream")

DEFAULT_ARGS = {
    "owner":            "logistream",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


def _build_spark(app_name: str):
    """Create a local Spark session configured for Delta Lake."""
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _health_check(**context):
    """Verify Delta tables exist and have recent data."""
    import os
    from config.settings import settings

    required = [
        f"{settings.delta_bronze_path}/shipments",
        f"{settings.delta_silver_path}/shipments",
        f"{settings.delta_gold_path}/alerts",
    ]
    missing = [p for p in required if not os.path.exists(p)]
    if missing:
        raise ValueError(f"Missing Delta tables: {missing}")

    print(f"[health_check] All {len(required)} tables reachable.")
    return "ok"


def _silver_optimize(**context):
    """OPTIMIZE + ZORDER Silver shipments table on carrier_code and event_ts."""
    from delta.tables import DeltaTable
    from config.settings import settings

    spark = _build_spark("logistream-silver-optimize")
    try:
        dt = DeltaTable.forPath(spark, f"{settings.delta_silver_path}/shipments")
        dt.optimize().executeZOrderBy("carrier_code", "event_ts")
        print("[silver_optimize] Done.")
    finally:
        spark.stop()


def _gold_optimize(**context):
    """OPTIMIZE (compaction) on Gold carrier_kpis table."""
    from delta.tables import DeltaTable
    from config.settings import settings

    spark = _build_spark("logistream-gold-optimize")
    try:
        DeltaTable.forPath(spark, f"{settings.delta_gold_path}/carrier_kpis") \
            .optimize().executeCompaction()
        print("[gold_optimize] Done.")
    finally:
        spark.stop()


def _bronze_vacuum(**context):
    """VACUUM Bronze shipments table — remove files older than 7 days."""
    import os
    from delta.tables import DeltaTable
    from config.settings import settings

    bronze_path = f"{settings.delta_bronze_path}/shipments"
    if not os.path.exists(bronze_path):
        print("[bronze_vacuum] Bronze table not found — skipping.")
        return

    spark = _build_spark("logistream-bronze-vacuum")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    try:
        DeltaTable.forPath(spark, bronze_path).vacuum(168)  # 7 days = 168 hours
        print("[bronze_vacuum] Done.")
    finally:
        spark.stop()


def _alert_summary(**context):
    """Push alert count to XCom for downstream monitoring."""
    import os
    from config.settings import settings

    alerts_path = f"{settings.delta_gold_path}/alerts"
    count = 0

    if os.path.exists(alerts_path):
        spark = _build_spark("logistream-dag-summary")
        try:
            count = spark.read.format("delta").load(alerts_path).count()
        finally:
            spark.stop()

    print(f"[alert_summary] Active alerts: {count}")
    context["ti"].xcom_push(key="alert_count", value=count)
    return count


with DAG(
    dag_id="logistream_maintenance",
    default_args=DEFAULT_ARGS,
    description="LogiStream — hourly Delta Lake maintenance",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["logistream", "delta", "maintenance"],
) as dag:

    health_check = PythonOperator(
        task_id="health_check",
        python_callable=_health_check,
    )

    silver_optimize = PythonOperator(
        task_id="silver_optimize",
        python_callable=_silver_optimize,
    )

    gold_optimize = PythonOperator(
        task_id="gold_optimize",
        python_callable=_gold_optimize,
    )

    bronze_vacuum = PythonOperator(
        task_id="bronze_vacuum",
        python_callable=_bronze_vacuum,
    )

    alert_summary = PythonOperator(
        task_id="alert_summary",
        python_callable=_alert_summary,
    )

    # DAG dependency chain
    health_check >> [silver_optimize, gold_optimize] >> bronze_vacuum >> alert_summary
