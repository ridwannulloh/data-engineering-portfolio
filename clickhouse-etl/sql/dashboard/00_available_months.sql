-- Months with published mart data; populates the dashboard period filter.
SELECT DISTINCT source_month
FROM mart.taxi_daily
ORDER BY source_month;
