"""Incremental staging -> mart transform driven by an ingestion-time watermark.

Rather than "aggregate yesterday", it recomputes every event_date that received rows since the
last run. Late-arriving events (event_time days in the past) are picked up automatically.
All timestamps stay server-side as epoch millis to avoid timezone/precision round-trips.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)

PIPELINE = "staging.events->mart.daily_event_stats"

_READ_WATERMARK = """
SELECT toUnixTimestamp64Milli(argMax(high_water, updated_at))
FROM etl.watermarks
WHERE pipeline = {pipeline:String}
"""

_AFFECTED_DAYS = """
SELECT DISTINCT toDate(event_time) AS d
FROM staging.events
WHERE _ingested_at >  fromUnixTimestamp64Milli({lo:Int64}, 'UTC')
  AND _ingested_at <= fromUnixTimestamp64Milli({hi:Int64}, 'UTC')
ORDER BY d
"""

# FINAL resolves ReplacingMergeTree duplicates that background merges haven't collapsed yet.
# It is restricted to a handful of days here, so the cost stays bounded.
_REBUILD_DAYS = """
INSERT INTO mart.daily_event_stats
    (event_date, event_type, country, events, users, revenue, _computed_at)
SELECT
    toDate(event_time) AS event_date,
    event_type,
    country,
    count(),
    uniqExact(user_id),
    sum(revenue),
    now64(3, 'UTC')
FROM staging.events FINAL
WHERE toDate(event_time) IN {days:Array(Date)}
GROUP BY event_date, event_type, country
"""

_WRITE_WATERMARK = """
INSERT INTO etl.watermarks (pipeline, high_water)
SELECT {pipeline:String}, fromUnixTimestamp64Milli({hi:Int64}, 'UTC')
"""


@dataclass(frozen=True)
class TransformResult:
    days: list[str]
    low_ms: int
    high_ms: int


def run_daily_stats(client: Client, *, safety_lag_s: int = 30) -> TransformResult:
    """safety_lag_s leaves a gap for inserts still in flight when the run starts."""
    params = {"pipeline": PIPELINE}
    lo = int(client.query(_READ_WATERMARK, parameters=params).first_row[0] or 0)
    hi = int(client.query("SELECT toUnixTimestamp64Milli(now64(3))").first_row[0])
    hi -= safety_lag_s * 1000
    if hi <= lo:
        return TransformResult([], lo, lo)

    days = [
        d.isoformat()
        for (d,) in client.query(_AFFECTED_DAYS, parameters={"lo": lo, "hi": hi}).result_rows
    ]
    if days:
        log.info("rebuilding %d day(s): %s..%s", len(days), days[0], days[-1])
        client.command(_REBUILD_DAYS, parameters={"days": days})
    # Advance only after the rebuild succeeded. A crash in between just recomputes the same
    # days next run, which ReplacingMergeTree makes harmless.
    client.command(_WRITE_WATERMARK, parameters={**params, "hi": hi})
    return TransformResult(days, lo, hi)
