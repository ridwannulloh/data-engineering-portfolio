-- Missing months have no row: compare coverage with reports/taxi-range.json.
SELECT source_month, quality_issue, count() AS source_rows
FROM raw.taxi_trips
WHERE source_month >= {start:Date}
  AND source_month <= {end:Date}
GROUP BY source_month, quality_issue
ORDER BY source_month, quality_issue;
