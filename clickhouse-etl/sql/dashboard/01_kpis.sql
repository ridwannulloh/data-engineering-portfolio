-- Valid trips only; USD amounts are recorded passenger charges, not driver profit.
-- {start:Date} and {end:Date} are first-of-month dates (the SQL Console prompts for them).
SELECT
    sum(trips) AS valid_trips,
    sum(total_amount) AS recorded_total_usd,
    round(sum(total_amount) / nullIf(sum(trips), 0), 2) AS average_total_usd,
    sum(tip_amount) AS recorded_tips_usd
FROM mart.taxi_daily
WHERE source_month >= {start:Date}
  AND source_month <= {end:Date};
