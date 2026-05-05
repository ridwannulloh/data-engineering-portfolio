"""
Synthetic supply chain event producer.

Generates realistic shipment lifecycle events and publishes them to Kafka.
Run: python ingest/producer.py [--rate 2] [--duration 60]
"""

import argparse
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from faker import Faker
from kafka import KafkaProducer

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.settings import settings
from ingest.schemas import (
    ShipmentEvent, CarrierUpdate, WarehouseOp,
    EventType, ServiceType, CarrierCode,
)

fake = Faker()

WAREHOUSES = ["WH-LAX", "WH-JFK", "WH-ORD", "WH-DFW", "WH-SEA", "WH-MIA"]
REGIONS = ["West", "East", "Central", "South", "Northwest", "Southeast"]

# Weighted event type distribution — reflects real logistics event mix
EVENT_WEIGHTS = {
    EventType.PICKUP:      0.10,
    EventType.IN_TRANSIT:  0.50,
    EventType.OUT_FOR_DEL: 0.15,
    EventType.DELIVERED:   0.15,
    EventType.FAILED_DEL:  0.03,
    EventType.EXCEPTION:   0.04,
    EventType.DELAY:       0.03,
}


def _make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",                   # wait for all replicas
        retries=3,
        linger_ms=10,                 # micro-batching for throughput
        compression_type="gzip",
    )


def _random_shipment_event(active_shipments: dict) -> ShipmentEvent:
    """Generate a realistic, stateful shipment event."""
    if active_shipments and random.random() < 0.7:
        # Progress an existing shipment
        sid = random.choice(list(active_shipments.keys()))
        last_state = active_shipments[sid]
        # Simple state machine progression
        progressions = {
            EventType.PICKUP:      EventType.IN_TRANSIT,
            EventType.IN_TRANSIT:  random.choice([
                EventType.IN_TRANSIT, EventType.OUT_FOR_DEL, EventType.DELAY
            ]),
            EventType.OUT_FOR_DEL: random.choice([
                EventType.DELIVERED, EventType.FAILED_DEL
            ]),
            EventType.DELAY:       EventType.IN_TRANSIT,
            EventType.EXCEPTION:   EventType.EXCEPTION,
        }
        event_type = progressions.get(last_state, EventType.IN_TRANSIT)
    else:
        sid = f"SHP-{uuid.uuid4().hex[:10].upper()}"
        event_type = EventType.PICKUP

    active_shipments[sid] = event_type
    # Remove terminal shipments
    if event_type in (EventType.DELIVERED, EventType.FAILED_DEL):
        active_shipments.pop(sid, None)

    service = random.choices(
        list(ServiceType), weights=[0.6, 0.3, 0.1]
    )[0]
    carrier = random.choice(list(CarrierCode))
    delay = random.choices([0, random.randint(30, 480)], weights=[0.8, 0.2])[0]

    now = datetime.now(timezone.utc)
    sla_map = {
        ServiceType.STANDARD:  settings.sla_standard_hours,
        ServiceType.EXPRESS:   settings.sla_express_hours,
        ServiceType.OVERNIGHT: settings.sla_overnight_hours,
    }
    eta = now + timedelta(hours=sla_map[service])

    return ShipmentEvent(
        shipment_id=sid,
        event_type=event_type.value,
        carrier_code=carrier.value,
        service_type=service.value,
        origin_wh=random.choice(WAREHOUSES),
        dest_city=fake.city(),
        dest_country="US",
        weight_kg=round(random.uniform(0.1, 50.0), 2),
        promised_eta=eta.isoformat(),
        event_ts=now.isoformat(),
        latitude=round(random.uniform(25.0, 48.0), 6),
        longitude=round(random.uniform(-125.0, -65.0), 6),
        delay_minutes=delay,
        notes=f"Exception: {fake.sentence()}" if event_type == EventType.EXCEPTION else "",
    )


def _random_carrier_update() -> CarrierUpdate:
    carrier = random.choice(list(CarrierCode))
    return CarrierUpdate(
        carrier_code=carrier.value,
        region=random.choice(REGIONS),
        active_shipments=random.randint(100, 5000),
        on_time_rate=round(random.uniform(0.75, 0.99), 3),
        avg_delay_minutes=round(random.uniform(0, 120), 1),
        report_ts=datetime.now(timezone.utc).isoformat(),
    )


def _random_warehouse_op(active_shipments: dict) -> WarehouseOp:
    sids = list(active_shipments.keys()) or [f"SHP-{uuid.uuid4().hex[:10].upper()}"]
    return WarehouseOp(
        warehouse_id=random.choice(WAREHOUSES),
        operation=random.choice(["INBOUND", "OUTBOUND", "SCAN", "HOLD"]),
        shipment_id=random.choice(sids),
        operator_id=f"OP-{random.randint(1000, 9999)}",
        dock_door=random.randint(1, 24),
        op_ts=datetime.now(timezone.utc).isoformat(),
    )


def run(events_per_second: float = 2.0, duration_seconds: int = 0):
    """
    Publish synthetic events to all three Kafka topics.

    Args:
        events_per_second: Target publish rate.
        duration_seconds:  Run duration (0 = run until Ctrl-C).
    """
    producer = _make_producer()
    active_shipments: dict = {}
    interval = 1.0 / events_per_second
    start = time.time()
    count = 0

    print(f"[LogiStream Producer] Publishing at {events_per_second} events/s "
          f"to {settings.kafka_bootstrap_servers}")

    try:
        while True:
            tick = time.time()

            # Publish shipment event (most frequent)
            evt = _random_shipment_event(active_shipments)
            producer.send(settings.kafka_topic_shipments, value=evt.to_json())

            # Carrier update every ~10 ticks
            if count % 10 == 0:
                upd = _random_carrier_update()
                producer.send(settings.kafka_topic_carriers, value=upd.to_json())

            # Warehouse op every ~5 ticks
            if count % 5 == 0:
                op = _random_warehouse_op(active_shipments)
                producer.send(settings.kafka_topic_warehouses, value=op.to_json())

            count += 1
            if count % 50 == 0:
                print(f"  [{count:,} events] active shipments: {len(active_shipments)}")

            if duration_seconds and (time.time() - start) >= duration_seconds:
                break

            elapsed = time.time() - tick
            if elapsed < interval:
                time.sleep(interval - elapsed)

    except KeyboardInterrupt:
        print("\n[Producer] Interrupted.")
    finally:
        producer.flush()
        producer.close()
        print(f"[Producer] Done. Total events: {count:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LogiStream Kafka producer")
    parser.add_argument("--rate", type=float, default=2.0,
                        help="Events per second (default: 2.0)")
    parser.add_argument("--duration", type=int, default=0,
                        help="Seconds to run (0 = forever)")
    args = parser.parse_args()
    run(events_per_second=args.rate, duration_seconds=args.duration)
