EXPLAIN indexes = 1
SELECT sum(total_amount)
FROM raw.taxi_trips
WHERE source_month = {month:Date}
  AND pickup_date = {day:Date}
  AND quality_issue = 'valid'
