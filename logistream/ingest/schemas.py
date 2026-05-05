"""
Event schemas for all Kafka topics.

These mirror the JSON payloads produced to Kafka and are used by both
the producer (serialisation) and Spark job (schema enforcement via StructType).
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
import json


class EventType(str, Enum):
    PICKUP       = "PICKUP"
    IN_TRANSIT   = "IN_TRANSIT"
    OUT_FOR_DEL  = "OUT_FOR_DELIVERY"
    DELIVERED    = "DELIVERED"
    FAILED_DEL   = "FAILED_DELIVERY"
    EXCEPTION    = "EXCEPTION"
    DELAY        = "DELAY"


class ServiceType(str, Enum):
    STANDARD  = "STANDARD"
    EXPRESS   = "EXPRESS"
    OVERNIGHT = "OVERNIGHT"


class CarrierCode(str, Enum):
    FEDEX = "FEDEX"
    UPS   = "UPS"
    DHL   = "DHL"
    USPS  = "USPS"


@dataclass
class ShipmentEvent:
    """Shipment lifecycle event — published to `shipment-events` topic."""
    shipment_id:    str
    event_type:     str          # EventType value
    carrier_code:   str          # CarrierCode value
    service_type:   str          # ServiceType value
    origin_wh:      str          # warehouse code e.g. "WH-LAX"
    dest_city:      str
    dest_country:   str
    weight_kg:      float
    promised_eta:   str          # ISO 8601 datetime
    event_ts:       str          # ISO 8601 datetime
    latitude:       float
    longitude:      float
    delay_minutes:  int = 0
    notes:          str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class CarrierUpdate:
    """Carrier status update — published to `carrier-updates` topic."""
    carrier_code:      str
    region:            str
    active_shipments:  int
    on_time_rate:      float     # 0.0–1.0
    avg_delay_minutes: float
    report_ts:         str       # ISO 8601 datetime

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class WarehouseOp:
    """Warehouse operational event — published to `warehouse-ops` topic."""
    warehouse_id:   str
    operation:      str          # INBOUND | OUTBOUND | SCAN | HOLD
    shipment_id:    str
    operator_id:    str
    dock_door:      int
    op_ts:          str          # ISO 8601 datetime

    def to_json(self) -> str:
        return json.dumps(asdict(self))
