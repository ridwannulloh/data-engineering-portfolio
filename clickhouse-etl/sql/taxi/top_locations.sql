-- Location IDs map to the official taxi_zone_lookup.csv linked in docs/nyc-taxi.md.
SELECT
    pickup_location_id,
    sum(trips) AS trips,
    sum(total_amount) AS total_usd,
    sum(tip_amount) AS recorded_tips_usd
FROM mart.taxi_daily
WHERE source_month = {month:Date}
GROUP BY pickup_location_id
ORDER BY trips DESC, pickup_location_id
LIMIT 10
