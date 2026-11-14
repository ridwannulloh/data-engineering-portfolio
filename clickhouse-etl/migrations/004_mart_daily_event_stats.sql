-- Daily aggregates, rebuilt per affected day by `ch-etl transform`.
-- Recomputing a day re-inserts its rows with a newer _computed_at; ReplacingMergeTree keeps the
-- newest, so reruns are idempotent. Query with FINAL (cheap on a table this small).
-- Caveat: a (day, event_type, country) group that disappears on recompute is not removed.
CREATE TABLE IF NOT EXISTS mart.daily_event_stats
(
    event_date   Date,
    event_type   LowCardinality(String),
    country      LowCardinality(String),
    events       UInt64,
    users        UInt64,
    revenue      Decimal(18, 4),
    _computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(_computed_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, country);
