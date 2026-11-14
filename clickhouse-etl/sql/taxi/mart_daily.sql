SELECT pickup_date, pickup_location_id, payment_type, trips, total_amount, tip_amount
FROM mart.taxi_daily
WHERE source_month = {month:Date}
ORDER BY pickup_date, pickup_location_id, payment_type
