SELECT
    pickup_date, pickup_location_id, payment_type,
    count() AS trips,
    sum(assumeNotNull(total_amount)) AS total_amount,
    sum(assumeNotNull(tip_amount)) AS tip_amount
FROM staging.taxi_trips
WHERE source_month = {month:Date}
GROUP BY pickup_date, pickup_location_id, payment_type
ORDER BY pickup_date, pickup_location_id, payment_type
