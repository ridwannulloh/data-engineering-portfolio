-- INSERT shape over the last 24h. Many inserts with a low avg_rows = part explosion risk.
-- On ClickHouse Cloud query_log is per replica; for the whole service use
-- clusterAllReplicas('default', system.query_log) instead of system.query_log.
SELECT
    arrayStringConcat(arraySort(tables), ', ') AS tables_touched,
    count()                                    AS inserts,
    sum(written_rows)                          AS rows_written,
    round(avg(written_rows))                   AS avg_rows,
    quantile(0.5)(query_duration_ms)           AS p50_ms,
    max(query_duration_ms)                     AS max_ms
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_kind = 'Insert'
  AND event_time > now() - INTERVAL 1 DAY
GROUP BY tables_touched
ORDER BY inserts DESC
LIMIT 20
