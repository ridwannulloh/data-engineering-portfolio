-- Active parts per partition. Each INSERT creates at least one part; merges shrink the count.
-- Hundreds of parts in one partition means inserts are too small or too frequent, or the
-- partition key is too fine. ClickHouse delays, then rejects, inserts when this grows too large.
SELECT
    database,
    table,
    partition,
    count()                                 AS active_parts,
    sum(rows)                               AS rows,
    formatReadableSize(sum(bytes_on_disk))  AS size_on_disk
FROM system.parts
WHERE active AND database IN ('raw', 'staging', 'mart', 'etl')
GROUP BY database, table, partition
ORDER BY active_parts DESC
LIMIT 20
