-- TLC payment_type codes: 0 flex fare/unspecified, 1 credit card, 2 cash, 3 no charge,
-- 4 dispute, 5 unknown, 6 voided trip.
SELECT payment_type, sum(trips) AS valid_trips, sum(total_amount) AS recorded_total_usd
FROM mart.taxi_daily
WHERE source_month >= {start:Date}
  AND source_month <= {end:Date}
GROUP BY payment_type
ORDER BY valid_trips DESC;
