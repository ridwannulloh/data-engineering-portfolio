-- Cleaned, typed, deduplicated events.
-- ReplacingMergeTree collapses rows with the same ORDER BY key during background merges,
-- keeping the highest _version. Every ORDER BY column must be immutable for a given event_id,
-- otherwise a corrected record lands under a different key and both copies survive.
CREATE TABLE IF NOT EXISTS staging.events
(
    event_id     UUID,
    event_time   DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    user_id      UInt64,
    event_type   LowCardinality(String),
    source       LowCardinality(String),
    revenue      Decimal(18, 4),
    country      LowCardinality(String),
    device       LowCardinality(String),
    _ingested_at DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    _version     UInt64,
    -- Lets the incremental transform find "rows ingested since the watermark" without a full scan.
    INDEX idx_ingested_at _ingested_at TYPE minmax GRANULARITY 4
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(event_time)
-- Low -> high cardinality: better compression and index pruning.
ORDER BY (event_type, toDate(event_time), event_id);

-- Insert-time transform: every block inserted into raw.events is cleaned and written here
-- in the same INSERT. No scheduler, no second pass over the data.
CREATE MATERIALIZED VIEW IF NOT EXISTS staging.events_mv TO staging.events AS
SELECT
    event_id,
    event_time,
    user_id,
    lower(trimBoth(event_type))           AS event_type,
    source,
    revenue,
    JSONExtractString(payload, 'country') AS country,
    JSONExtractString(payload, 'device')  AS device,
    _ingested_at,
    toUnixTimestamp64Milli(_ingested_at)  AS _version
FROM raw.events
WHERE event_id != toUUID('00000000-0000-0000-0000-000000000000');
