"""
tests/test_transformations.py
──────────────────────────────
Unit tests for Silver and Gold transformation logic.
No external dependencies required — all tests use in-memory data.

Run:
    pytest tests/ -v
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from pipelines.bronze_to_silver import transform_orders, transform_inventory, stock_risk_level
from pipelines.silver_to_gold   import build_gmv_hourly, build_rfm


# ── Fixtures ───────────────────────────────────────────────────────────────

def make_order_record(**overrides) -> dict:
    base = {
        "event_type":   "order_placed",
        "order_id":     "ORD-TEST001",
        "customer_id":  "CUST-000001",
        "status":       "placed",
        "channel":      "web",
        "total_amount": 120.50,
        "currency":     "USD",
        "created_at":   "2024-06-15T10:30:00+00:00",
        "items": [{"product_id": "PROD_0001", "quantity": 2, "unit_price": 60.25, "subtotal": 120.50}],
        "_ingested_at": "2024-06-15T10:31:00+00:00",
    }
    return {**base, **overrides}


def make_inv_record(**overrides) -> dict:
    base = {
        "product_id":     "PROD_0001",
        "warehouse_id":   "WH-01",
        "category":       "Electronics",
        "event_type":     "sale",
        "delta_quantity":  -3,
        "quantity_after":  47,
        "low_stock_flag":  False,
        "stockout_flag":   False,
        "recorded_at":    "2024-06-15T10:30:00+00:00",
        "_ingested_at":   "2024-06-15T10:31:00+00:00",
    }
    return {**base, **overrides}


# ── Bronze → Silver: Orders ────────────────────────────────────────────────

class TestTransformOrders:
    def test_valid_order_parsed_correctly(self):
        records   = [make_order_record()]
        df, rejected = transform_orders(records)
        assert len(df) == 1
        assert len(rejected) == 0
        assert df.iloc[0]["channel"] == "web"
        assert df.iloc[0]["total_amount"] == 120.50

    def test_derived_columns_present(self):
        records   = [make_order_record()]
        df, _     = transform_orders(records)
        assert "order_hour" in df.columns
        assert "order_day_of_week" in df.columns
        assert "is_weekend" in df.columns

    def test_weekend_flag_correct(self):
        # 2024-06-15 is a Saturday
        records   = [make_order_record(created_at="2024-06-15T10:00:00+00:00")]
        df, _     = transform_orders(records)
        assert bool(df.iloc[0]["is_weekend"]) is True

    def test_weekday_flag_correct(self):
        # 2024-06-17 is a Monday
        records   = [make_order_record(created_at="2024-06-17T10:00:00+00:00")]
        df, _     = transform_orders(records)
        assert bool(df.iloc[0]["is_weekend"]) is False

    def test_non_placement_events_skipped(self):
        records   = [make_order_record(event_type="order_status_update")]
        df, rejected = transform_orders(records)
        assert df.empty
        assert len(rejected) == 0   # silently skipped, not quarantined

    def test_missing_required_field_quarantined(self):
        record = make_order_record()
        del record["order_id"]
        df, rejected = transform_orders([record])
        assert df.empty
        assert len(rejected) == 1
        assert rejected[0]["_reject_reason"] == "missing_required_fields"

    def test_invalid_timestamp_quarantined(self):
        records   = [make_order_record(created_at="not-a-date")]
        df, rejected = transform_orders(records)
        assert df.empty
        assert rejected[0]["_reject_reason"] == "invalid_timestamp"

    def test_channel_normalised_to_lowercase(self):
        records   = [make_order_record(channel="WEB")]
        df, _     = transform_orders(records)
        assert df.iloc[0]["channel"] == "web"


# ── Bronze → Silver: Inventory ─────────────────────────────────────────────

class TestTransformInventory:
    def test_valid_record_transforms(self):
        df = transform_inventory([make_inv_record()])
        assert len(df) == 1
        assert df.iloc[0]["stock_risk_level"] == "OK"

    def test_stock_risk_levels(self):
        assert stock_risk_level(0)  == "STOCKOUT"
        assert stock_risk_level(5)  == "CRITICAL"
        assert stock_risk_level(15) == "LOW"
        assert stock_risk_level(50) == "OK"

    def test_deduplication(self):
        rec1 = make_inv_record(recorded_at="2024-06-15T10:00:00+00:00")
        rec2 = make_inv_record(recorded_at="2024-06-15T10:00:00+00:00")   # duplicate
        df   = transform_inventory([rec1, rec2])
        assert len(df) == 1

    def test_records_without_required_fields_skipped(self):
        df = transform_inventory([{"event_type": "sale"}])
        assert df.empty


# ── Silver → Gold: GMV ────────────────────────────────────────────────────

class TestGMVHourly:
    def _order_df(self):
        return pd.DataFrame([
            {"order_id": "O1", "customer_id": "C1", "channel": "web",    "total_amount": 100.0, "created_at": "2024-06-15T10:00:00+00:00"},
            {"order_id": "O2", "customer_id": "C2", "channel": "web",    "total_amount": 200.0, "created_at": "2024-06-15T10:30:00+00:00"},
            {"order_id": "O3", "customer_id": "C3", "channel": "mobile", "total_amount": 50.0,  "created_at": "2024-06-15T10:45:00+00:00"},
        ])

    def test_gmv_aggregated_by_channel(self):
        df = build_gmv_hourly(self._order_df())
        web_row = df[df["channel"] == "web"].iloc[0]
        assert web_row["total_gmv"] == 300.0
        assert web_row["order_count"] == 2

    def test_avg_order_value_correct(self):
        df      = build_gmv_hourly(self._order_df())
        web_row = df[df["channel"] == "web"].iloc[0]
        assert web_row["avg_order_value"] == 150.0

    def test_empty_input_returns_empty(self):
        df = build_gmv_hourly(pd.DataFrame())
        assert df.empty


# ── Silver → Gold: RFM ────────────────────────────────────────────────────

class TestRFM:
    def _order_df(self):
        snapshot = datetime(2024, 6, 15, tzinfo=timezone.utc)
        return pd.DataFrame([
            # Champion: recent, frequent, high spend
            {"order_id": "O1", "customer_id": "C1", "total_amount": 500.0, "created_at": "2024-06-14T10:00:00+00:00"},
            {"order_id": "O2", "customer_id": "C1", "total_amount": 500.0, "created_at": "2024-06-13T10:00:00+00:00"},
            {"order_id": "O3", "customer_id": "C1", "total_amount": 500.0, "created_at": "2024-06-12T10:00:00+00:00"},
            # Lost: old, single purchase, low spend
            {"order_id": "O4", "customer_id": "C2", "total_amount": 10.0,  "created_at": "2024-01-01T10:00:00+00:00"},
            # Middle: moderate
            {"order_id": "O5", "customer_id": "C3", "total_amount": 150.0, "created_at": "2024-05-01T10:00:00+00:00"},
            {"order_id": "O6", "customer_id": "C3", "total_amount": 150.0, "created_at": "2024-05-15T10:00:00+00:00"},
        ])

    def test_rfm_scores_generated(self):
        snapshot = datetime(2024, 6, 15, tzinfo=timezone.utc)
        df       = build_rfm(self._order_df(), snapshot_date=snapshot)
        assert len(df) == 3
        assert {"r_score", "f_score", "m_score", "rfm_score", "segment"}.issubset(df.columns)

    def test_scores_in_valid_range(self):
        snapshot = datetime(2024, 6, 15, tzinfo=timezone.utc)
        df       = build_rfm(self._order_df(), snapshot_date=snapshot)
        for col in ["r_score", "f_score", "m_score"]:
            assert df[col].between(1, 5).all(), f"{col} out of range"

    def test_empty_input_returns_empty(self):
        df = build_rfm(pd.DataFrame())
        assert df.empty
