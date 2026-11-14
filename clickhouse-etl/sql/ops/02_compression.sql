-- Per-column compression. Low ratios on big columns are candidates for better codecs,
-- LowCardinality, or narrower types.
SELECT
    database,
    table,
    name                                                              AS column,
    type,
    compression_codec,
    formatReadableSize(data_compressed_bytes)                         AS compressed,
    formatReadableSize(data_uncompressed_bytes)                       AS uncompressed,
    round(data_uncompressed_bytes / nullIf(data_compressed_bytes, 0), 1) AS ratio
FROM system.columns
WHERE database IN ('raw', 'staging', 'mart', 'etl') AND data_compressed_bytes > 0
ORDER BY data_compressed_bytes DESC
LIMIT 20
