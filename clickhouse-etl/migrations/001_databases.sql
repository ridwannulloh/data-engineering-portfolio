-- Layered layout: raw (landing, append-only) -> staging (clean, deduplicated) -> mart (aggregates).
-- On ClickHouse Cloud every MergeTree table becomes SharedMergeTree automatically; no ON CLUSTER needed.
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS staging;
CREATE DATABASE IF NOT EXISTS mart;
CREATE DATABASE IF NOT EXISTS etl;
