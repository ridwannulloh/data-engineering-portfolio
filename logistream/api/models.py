"""Pydantic response models for the LogiStream API."""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field


class Alert(BaseModel):
    shipment_id:     str
    event_type:      str
    carrier_code:    str
    service_type:    str
    delay_minutes:   int
    delay_severity:  str
    sla_breached:    bool
    origin_wh:       str
    dest_city:       str
    event_ts:        str
    notes:           Optional[str] = ""
    alert_generated_at: str


class AlertsResponse(BaseModel):
    count:        int
    generated_at: str
    alerts:       List[Alert]


class CarrierKPI(BaseModel):
    carrier_code:       str
    window_start:       str
    window_end:         str
    total_events:       int
    delayed_events:     int
    avg_delay_minutes:  float
    sla_breach_rate:    float = Field(..., description="0.0–1.0")
    p95_delay_minutes:  float


class MetricsResponse(BaseModel):
    window_minutes: int
    generated_at:   str
    carriers:       List[CarrierKPI]


class HealthResponse(BaseModel):
    status:          str          # "healthy" | "degraded" | "down"
    delta_reachable: bool
    tables_found:    List[str]
    checked_at:      str
