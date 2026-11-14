-- High-water marks for incremental transforms. Append a row per advance; read with argMax.
CREATE TABLE IF NOT EXISTS etl.watermarks
(
    pipeline   LowCardinality(String),
    high_water DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY pipeline;

-- One row per pipeline run. Written with async_insert so single-row inserts don't create a part each.
CREATE TABLE IF NOT EXISTS etl.runs
(
    run_id      UUID,
    pipeline    LowCardinality(String),
    status      LowCardinality(String),
    rows        UInt64,
    started_at  DateTime64(3, 'UTC'),
    finished_at DateTime64(3, 'UTC'),
    error       String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (pipeline, started_at)
TTL toDateTime(started_at) + INTERVAL 180 DAY DELETE;
