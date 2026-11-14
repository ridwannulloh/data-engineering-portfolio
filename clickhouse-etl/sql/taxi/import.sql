-- ClickHouse reads Parquet directly over HTTPS; no Kaggle account or Python dataframe needed.
-- NYC timestamps are local wall times. Parse their text in America/New_York explicitly.
INSERT INTO raw.{target:Identifier}
WITH parsed AS
(
    SELECT
        {month:Date} AS source_month,
        parseDateTime64BestEffortOrNull(toString(tpep_pickup_datetime), 3,
                                      'America/New_York') AS pickup_at,
        parseDateTime64BestEffortOrNull(toString(tpep_dropoff_datetime), 3,
                                      'America/New_York') AS dropoff_at,
        ifNull(accurateCastOrNull(PULocationID, 'UInt16'), 0) AS pickup_location_id,
        ifNull(accurateCastOrNull(DOLocationID, 'UInt16'), 0) AS dropoff_location_id,
        ifNull(accurateCastOrNull(payment_type, 'UInt8'), 0) AS payment_code,
        accurateCastOrNull(passenger_count, 'Int16') AS passengers,
        toFloat64(trip_distance) AS distance,
        toDecimal64OrNull(toString(round(fare_amount, 2)), 2) AS fare,
        toDecimal64OrNull(toString(round(tip_amount, 2)), 2) AS tip,
        toDecimal64OrNull(toString(round(total_amount, 2)), 2) AS total
    FROM url({url:String}, 'Parquet')
)
SELECT
    source_month,
    pickup_at,
    dropoff_at,
    ifNull(toDate(pickup_at), source_month) AS pickup_date,
    pickup_location_id,
    dropoff_location_id,
    payment_code,
    passengers,
    distance,
    fare,
    tip,
    total,
    multiIf(
        isNull(pickup_at) OR isNull(dropoff_at), 'missing_time',
        toStartOfMonth(pickup_at) != source_month, 'outside_source_month',
        dropoff_at < pickup_at, 'negative_duration',
        isNull(distance) OR NOT isFinite(distance) OR distance < 0, 'invalid_distance',
        isNull(fare) OR isNull(tip) OR isNull(total), 'missing_or_invalid_amount',
        fare < 0 OR tip < 0 OR total < 0, 'negative_amount',
        pickup_location_id NOT BETWEEN 1 AND 265
            OR dropoff_location_id NOT BETWEEN 1 AND 265, 'invalid_location',
        'valid'
    ) AS quality_issue,
    {url:String} AS source_url
FROM parsed
