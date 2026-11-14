-- Hours are NYC local time. Amounts are USD; total_amount is not driver net income.
SELECT
    toHour(pickup_at) AS pickup_hour_nyc,
    count() AS trips,
    round(avg(total_amount), 2) AS average_total_usd,
    quantiles(0.5, 0.95)(trip_distance) AS distance_miles_p50_p95
FROM staging.taxi_trips
WHERE source_month = {month:Date}
GROUP BY pickup_hour_nyc
ORDER BY trips DESC
