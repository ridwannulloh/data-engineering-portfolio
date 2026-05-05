"""
Delta Lake query helpers for the FastAPI service.

Uses PySpark in local mode to read Gold tables.
In production, replace with DeltaSharing, Trino, or DuckDB reads
for lower latency API responses.
"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional
import os

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.settings import settings
from api.models import Alert, CarrierKPI, HealthResponse


def _get_spark():
    """Lazy Spark session for Delta reads — reuse if already initialised."""
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .appName("logistream-api")
        .master("local[2]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def fetch_active_alerts(
    carrier: Optional[str] = None,
    severity: Optional[str] = None,
    limit: int = 50,
) -> List[Alert]:
    alerts_path = f"{settings.delta_gold_path}/alerts"
    if not os.path.exists(alerts_path):
        return []

    spark = _get_spark()
    df = spark.read.format("delta").load(alerts_path)

    if carrier:
        df = df.filter(df.carrier_code == carrier.upper())
    if severity:
        df = df.filter(df.delay_severity == severity.upper())

    rows = df.orderBy(df.alert_generated_at.desc()).limit(limit).collect()
    return [
        Alert(
            shipment_id=r.shipment_id,
            event_type=r.event_type,
            carrier_code=r.carrier_code,
            service_type=r.service_type,
            delay_minutes=int(r.delay_minutes or 0),
            delay_severity=r.delay_severity,
            sla_breached=bool(r.sla_breached),
            origin_wh=r.origin_wh or "",
            dest_city=r.dest_city or "",
            event_ts=str(r.event_ts),
            notes=r.notes or "",
            alert_generated_at=str(r.alert_generated_at),
        )
        for r in rows
    ]


def fetch_carrier_kpis(
    carrier: Optional[str] = None,
    window_minutes: int = 60,
) -> List[CarrierKPI]:
    kpis_path = f"{settings.delta_gold_path}/carrier_kpis"
    if not os.path.exists(kpis_path):
        return []

    spark = _get_spark()
    from pyspark.sql import functions as F

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    df = (
        spark.read.format("delta").load(kpis_path)
        .filter(F.col("window_start") >= cutoff.isoformat())
    )

    if carrier:
        df = df.filter(df.carrier_code == carrier.upper())

    rows = df.orderBy(df.window_start.desc()).collect()
    return [
        CarrierKPI(
            carrier_code=r.carrier_code,
            window_start=str(r.window_start),
            window_end=str(r.window_end),
            total_events=int(r.total_events),
            delayed_events=int(r.delayed_events),
            avg_delay_minutes=float(r.avg_delay_minutes or 0),
            sla_breach_rate=float(r.sla_breach_rate or 0),
            p95_delay_minutes=float(r.p95_delay_minutes or 0),
        )
        for r in rows
    ]


def check_pipeline_health() -> HealthResponse:
    tables = {
        "bronze/shipments": f"{settings.delta_bronze_path}/shipments",
        "silver/shipments": f"{settings.delta_silver_path}/shipments",
        "gold/alerts":      f"{settings.delta_gold_path}/alerts",
        "gold/carrier_kpis": f"{settings.delta_gold_path}/carrier_kpis",
    }
    found = [name for name, path in tables.items() if os.path.exists(path)]
    reachable = len(found) > 0
    status = "healthy" if len(found) == len(tables) else (
        "degraded" if reachable else "down"
    )
    return HealthResponse(
        status=status,
        delta_reachable=reachable,
        tables_found=found,
        checked_at=datetime.now(timezone.utc).isoformat(),
    )
