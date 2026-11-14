-- Landing table: exactly what the source sent, plus lineage columns.
-- Optimised for cheap, large appends and short retention, not for querying.
CREATE TABLE IF NOT EXISTS raw.events
(
    event_id     UUID,
    event_time   DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    user_id      UInt64,
    -- LowCardinality: dictionary-encoded, much smaller and faster for < ~10k distinct values.
    event_type   LowCardinality(String),
    source       LowCardinality(String),
    -- Defaults instead of Nullable: Nullable adds a separate null-map column per value.
    revenue      Decimal(18, 4) DEFAULT 0,
    payload      String CODEC(ZSTD(3)),
    _batch_id    String,
    _ingested_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD(1))
)
ENGINE = MergeTree
-- Coarse (monthly) partitions. Fine-grained partitions = too many parts = throttled inserts.
PARTITION BY toYYYYMM(_ingested_at)
ORDER BY (source, event_type, _ingested_at)
-- Raw is a replay buffer. ttl_only_drop_parts drops whole expired parts instead of rewriting them.
TTL toDateTime(_ingested_at) + INTERVAL 30 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;
