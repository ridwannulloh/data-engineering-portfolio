-- Merges in flight. Persistent long merges on staging usually mean ingest outpaces merging.
SELECT
    database,
    table,
    round(elapsed, 1)                                   AS elapsed_s,
    round(progress * 100, 1)                            AS pct,
    num_parts,
    formatReadableSize(total_size_bytes_compressed)     AS size
FROM system.merges
ORDER BY elapsed DESC
