"""Synthetic event source with the problems real sources have: duplicate deliveries,
late-arriving events and inconsistent casing. Swap this for your real extractor."""

from __future__ import annotations

import json
import random
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

RAW_COLUMNS = [
    "event_id",
    "event_time",
    "user_id",
    "event_type",
    "source",
    "revenue",
    "payload",
    "_batch_id",
]

_EVENT_TYPES = ["page_view"] * 6 + ["click"] * 3 + ["signup", " Purchase", "PURCHASE", "purchase"]
_SOURCES = ["web", "ios", "android"]
_COUNTRIES = ["ID", "SG", "MY", "TH", "VN", "PH"]
_DEVICES = ["mobile", "desktop", "tablet"]


def generate_events(
    n: int,
    *,
    batch_id: str,
    seed: int | None = None,
    dup_rate: float = 0.02,
    late_rate: float = 0.05,
) -> Iterator[tuple]:
    rng = random.Random(seed)
    now = datetime.now(UTC)
    recent: list[tuple] = []
    for _ in range(n):
        if recent and rng.random() < dup_rate:
            # At-least-once delivery: the exact same event shows up again.
            yield rng.choice(recent)
            continue
        if rng.random() < late_rate:
            lag = timedelta(days=rng.uniform(1, 3))
        else:
            lag = timedelta(seconds=rng.uniform(0, 3600))
        event_type = rng.choice(_EVENT_TYPES)
        is_purchase = event_type.strip().lower() == "purchase"
        row = (
            uuid.UUID(int=rng.getrandbits(128), version=4),
            now - lag,
            rng.randint(1, 50_000),
            event_type,
            rng.choice(_SOURCES),
            Decimal(f"{rng.uniform(5, 500):.2f}") if is_purchase else Decimal(0),
            json.dumps({"country": rng.choice(_COUNTRIES), "device": rng.choice(_DEVICES)}),
            batch_id,
        )
        if len(recent) < 1000:
            recent.append(row)
        else:
            recent[rng.randrange(1000)] = row
        yield row
