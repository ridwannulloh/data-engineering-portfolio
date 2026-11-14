SELECT
    pickup_date,
    sum(trips) AS valid_trips,
    sum(total_amount) AS recorded_total_usd,
    sum(tip_amount) AS recorded_tips_usd
FROM mart.taxi_daily
WHERE source_month >= {start:Date}
  AND source_month <= {end:Date}
GROUP BY pickup_date
ORDER BY pickup_date;
