"""
LogiStream FastAPI Alert Service.

Exposes Gold-layer Delta Lake data via REST endpoints.
Demonstrates API-building skills clients can verify at /docs.

Run: uvicorn api.main:app --reload
"""

from datetime import datetime, timezone
from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.models import (
    Alert, CarrierKPI, HealthResponse, AlertsResponse, MetricsResponse,
)
from api.queries import (
    fetch_active_alerts, fetch_carrier_kpis, check_pipeline_health,
)

app = FastAPI(
    title="LogiStream Alert Service",
    description=(
        "Real-time supply chain alert API backed by Delta Lake Gold tables. "
        "Surfaces delay alerts and carrier KPIs from a Kafka + Spark Structured Streaming pipeline."
    ),
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse, tags=["system"])
def health():
    """Pipeline health check — confirms Delta tables are reachable."""
    return check_pipeline_health()


@app.get("/alerts", response_model=AlertsResponse, tags=["alerts"])
def get_alerts(
    carrier: Optional[str] = Query(None, description="Filter by carrier code"),
    severity: Optional[str] = Query(None, description="NONE | LOW | MEDIUM | HIGH"),
    limit: int = Query(50, ge=1, le=500, description="Max results"),
):
    """
    Return active delay alerts from the Gold layer.

    Alerts are shipments with EXCEPTION events, HIGH delay severity,
    or confirmed SLA breaches.
    """
    alerts = fetch_active_alerts(carrier=carrier, severity=severity, limit=limit)
    return AlertsResponse(
        count=len(alerts),
        generated_at=datetime.now(timezone.utc).isoformat(),
        alerts=alerts,
    )


@app.get("/metrics", response_model=MetricsResponse, tags=["metrics"])
def get_metrics(
    carrier: Optional[str] = Query(None, description="Filter by carrier code"),
    window_minutes: int = Query(
        60, ge=5, le=1440,
        description="Look-back window in minutes for aggregated KPIs",
    ),
):
    """
    Return carrier performance KPIs aggregated over the specified window.

    Metrics are computed from 5-minute tumbling window aggregations written
    to the Gold carrier_kpis Delta table.
    """
    kpis = fetch_carrier_kpis(carrier=carrier, window_minutes=window_minutes)
    return MetricsResponse(
        window_minutes=window_minutes,
        generated_at=datetime.now(timezone.utc).isoformat(),
        carriers=kpis,
    )
