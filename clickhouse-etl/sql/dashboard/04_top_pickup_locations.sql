-- Location IDs only; `ch-etl dashboard` adds zone names from the TLC lookup file.
SELECT pickup_location_id, sum(trips) AS valid_trips, sum(total_amount) AS recorded_total_usd
FROM mart.taxi_daily
WHERE source_month >= {start:Date}
  AND source_month <= {end:Date}
GROUP BY pickup_location_id
ORDER BY valid_trips DESC
LIMIT 10;
