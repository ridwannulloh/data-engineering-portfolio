-- A monthly snapshot of selected columns from the official NYC TLC Yellow Taxi files.
-- load-taxi publishes a complete month with REPLACE PARTITION, never append/retry tokens.
CREATE TABLE IF NOT EXISTS raw.taxi_trips
(
    source_month Date,
    pickup_at Nullable(DateTime64(3, 'America/New_York')) CODEC(ZSTD(1)),
    dropoff_at Nullable(DateTime64(3, 'America/New_York')) CODEC(ZSTD(1)),
    pickup_date Date,
    pickup_location_id UInt16,
    dropoff_location_id UInt16,
    payment_type UInt8,
    passenger_count Nullable(Int16),
    trip_distance Nullable(Float64) CODEC(ZSTD(1)),
    fare_amount Nullable(Decimal(18, 2)),
    tip_amount Nullable(Decimal(18, 2)),
    total_amount Nullable(Decimal(18, 2)),
    quality_issue LowCardinality(String),
    source_url LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(source_month)
ORDER BY (pickup_date, pickup_location_id);

-- Keep excluded records in raw so the quality report can account for every source row.
CREATE VIEW IF NOT EXISTS staging.taxi_trips AS
SELECT * FROM raw.taxi_trips WHERE quality_issue = 'valid';

CREATE TABLE IF NOT EXISTS mart.taxi_daily
(
    source_month Date,
    pickup_date Date,
    pickup_location_id UInt16,
    payment_type UInt8,
    trips UInt64,
    total_amount Decimal(38, 2),
    tip_amount Decimal(38, 2)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(source_month)
ORDER BY (source_month, pickup_date, pickup_location_id, payment_type);

-- Full atomic replacement handles reruns and groups removed by a corrected source file.
-- Daily automatic refresh; load-taxi and taxi-demo also request a refresh and wait for it.
-- This is a refreshable MV, not an insert-triggered incremental MV.
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.taxi_daily_mv
REFRESH EVERY 1 DAY TO mart.taxi_daily AS
SELECT
    source_month,
    pickup_date,
    pickup_location_id,
    payment_type,
    count() AS trips,
    sum(assumeNotNull(total_amount)) AS total_amount,
    sum(assumeNotNull(tip_amount)) AS tip_amount
FROM staging.taxi_trips
GROUP BY source_month, pickup_date, pickup_location_id, payment_type
SETTINGS select_sequential_consistency = 1;
