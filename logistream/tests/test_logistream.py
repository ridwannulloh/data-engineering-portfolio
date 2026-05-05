"""
LogiStream test suite.

Tests the transform logic in PySpark local mode — no Kafka or Delta required.
Run: pytest tests/ -v
"""

import json
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Local Spark session for tests — no Delta, no Kafka."""
    return (
        SparkSession.builder
        .appName("logistream-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _make_shipment_rows(spark, overrides: list[dict]):
    """Create a test DataFrame matching the Silver schema."""
    now = datetime.now(timezone.utc)
    eta = now + timedelta(hours=48)

    base = {
        "shipment_id":   "SHP-TEST001",
        "event_type":    "IN_TRANSIT",
        "carrier_code":  "FEDEX",
        "service_type":  "STANDARD",
        "origin_wh":     "WH-LAX",
        "dest_city":     "Chicago",
        "dest_country":  "US",
        "weight_kg":     5.0,
        "promised_eta":  eta.isoformat(),
        "event_ts":      now.isoformat(),
        "latitude":      34.05,
        "longitude":     -118.24,
        "delay_minutes": 0,
        "notes":         "",
        "kafka_ts":      now,
        "partition":     0,
        "offset":        0,
        "ingested_at":   now,
    }
    rows = [{**base, **ov} for ov in overrides]
    return spark.createDataFrame(rows)


# ── Transform tests ──────────────────────────────────────────────────────────

class TestCleanAndEnrich:
    def test_zero_delay_no_breach(self, spark):
        from spark.transforms import clean_and_enrich
        df = _make_shipment_rows(spark, [{"delay_minutes": 0}])
        result = clean_and_enrich(df).collect()
        assert result[0]["sla_breached"] is False
        assert result[0]["delay_severity"] == "NONE"

    def test_high_delay_breach_standard(self, spark):
        from spark.transforms import clean_and_enrich
        # STANDARD SLA = 48h = 2880 min; delay 3000 min > SLA
        df = _make_shipment_rows(spark, [{
            "service_type":  "STANDARD",
            "delay_minutes": 3000,
        }])
        result = clean_and_enrich(df).collect()
        assert result[0]["sla_breached"] is True
        assert result[0]["delay_severity"] == "HIGH"

    def test_express_sla_stricter(self, spark):
        from spark.transforms import clean_and_enrich
        # EXPRESS SLA = 24h = 1440 min; delay 1500 min breaches it
        df = _make_shipment_rows(spark, [{
            "service_type":  "EXPRESS",
            "delay_minutes": 1500,
        }])
        result = clean_and_enrich(df).collect()
        assert result[0]["sla_breached"] is True

    def test_medium_delay_classification(self, spark):
        from spark.transforms import clean_and_enrich
        df = _make_shipment_rows(spark, [{"delay_minutes": 90}])
        result = clean_and_enrich(df).collect()
        assert result[0]["delay_severity"] == "MEDIUM"

    def test_geo_region_west(self, spark):
        from spark.transforms import clean_and_enrich
        df = _make_shipment_rows(spark, [{"longitude": -120.0}])
        result = clean_and_enrich(df).collect()
        assert result[0]["geo_region"] == "West"

    def test_geo_region_east(self, spark):
        from spark.transforms import clean_and_enrich
        df = _make_shipment_rows(spark, [{"longitude": -75.0}])
        result = clean_and_enrich(df).collect()
        assert result[0]["geo_region"] == "East"

    def test_deduplication(self, spark):
        from spark.transforms import clean_and_enrich
        now = datetime.now(timezone.utc).isoformat()
        # Two identical rows — should deduplicate to one
        df = _make_shipment_rows(spark, [
            {"shipment_id": "SHP-DUP", "event_ts": now},
            {"shipment_id": "SHP-DUP", "event_ts": now},
        ])
        result = clean_and_enrich(df).collect()
        assert len(result) == 1

    def test_null_shipment_id_filtered(self, spark):
        from spark.transforms import clean_and_enrich
        df = _make_shipment_rows(spark, [{"shipment_id": None}])
        result = clean_and_enrich(df).collect()
        assert len(result) == 0


class TestActiveAlerts:
    def test_exception_event_generates_alert(self, spark):
        from spark.transforms import clean_and_enrich, active_alerts
        df = _make_shipment_rows(spark, [{
            "event_type":    "EXCEPTION",
            "delay_minutes": 0,
        }])
        silver = clean_and_enrich(df)
        result = active_alerts(silver).collect()
        assert len(result) == 1
        assert result[0]["event_type"] == "EXCEPTION"

    def test_high_delay_generates_alert(self, spark):
        from spark.transforms import clean_and_enrich, active_alerts
        df = _make_shipment_rows(spark, [{"delay_minutes": 500}])
        silver = clean_and_enrich(df)
        result = active_alerts(silver).collect()
        assert len(result) == 1

    def test_low_delay_no_alert(self, spark):
        from spark.transforms import clean_and_enrich, active_alerts
        df = _make_shipment_rows(spark, [{"delay_minutes": 30, "event_type": "IN_TRANSIT"}])
        silver = clean_and_enrich(df)
        result = active_alerts(silver).collect()
        assert len(result) == 0


# ── Producer tests (no Kafka) ─────────────────────────────────────────────────

class TestProducer:
    def test_shipment_event_serialisable(self):
        from ingest.schemas import ShipmentEvent
        evt = ShipmentEvent(
            shipment_id="SHP-X",
            event_type="PICKUP",
            carrier_code="FEDEX",
            service_type="STANDARD",
            origin_wh="WH-LAX",
            dest_city="Seattle",
            dest_country="US",
            weight_kg=2.5,
            promised_eta=datetime.now(timezone.utc).isoformat(),
            event_ts=datetime.now(timezone.utc).isoformat(),
            latitude=47.6,
            longitude=-122.3,
        )
        payload = json.loads(evt.to_json())
        assert payload["shipment_id"] == "SHP-X"
        assert payload["carrier_code"] == "FEDEX"

    def test_event_state_machine_pickup_first(self):
        from ingest.producer import _random_shipment_event
        active: dict = {}
        # First call with empty state must produce a PICKUP
        evt = _random_shipment_event(active)
        assert evt.event_type == "PICKUP"
        assert evt.shipment_id in active


# ── API tests (no Delta Lake) ─────────────────────────────────────────────────

class TestAPI:
    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)

    def test_health_endpoint_reachable(self, client):
        with patch("api.queries.check_pipeline_health") as mock_health:
            mock_health.return_value = MagicMock(
                status="healthy",
                delta_reachable=True,
                tables_found=["bronze/shipments"],
                checked_at="2024-01-01T00:00:00Z",
            )
            resp = client.get("/health")
        assert resp.status_code == 200

    def test_alerts_endpoint_returns_list(self, client):
        with patch("api.queries.fetch_active_alerts", return_value=[]):
            resp = client.get("/alerts")
        assert resp.status_code == 200
        data = resp.json()
        assert "alerts" in data
        assert "count" in data

    def test_metrics_endpoint_returns_list(self, client):
        with patch("api.queries.fetch_carrier_kpis", return_value=[]):
            resp = client.get("/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert "carriers" in data
        assert "window_minutes" in data

    def test_alerts_filter_by_carrier(self, client):
        with patch("api.queries.fetch_active_alerts", return_value=[]) as mock:
            client.get("/alerts?carrier=FEDEX")
            mock.assert_called_once_with(carrier="FEDEX", severity=None, limit=50)

    def test_alerts_limit_validation(self, client):
        with patch("api.queries.fetch_active_alerts", return_value=[]):
            resp = client.get("/alerts?limit=0")
        assert resp.status_code == 422   # Pydantic validation error
