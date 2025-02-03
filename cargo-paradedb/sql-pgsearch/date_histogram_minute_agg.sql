SELECT 
    date_trunc('minute', "timestamp") AS minute_bucket,
    COUNT(*) AS doc_count
FROM benchmark_eslogs
WHERE benchmark_eslogs @@@ paradedb.range(
    field => 'timestamp',
    range => '[2023-01-01T00:00:00Z,2023-01-03T00:00:00Z)'::tstzrange
)
GROUP BY 1
ORDER BY 1;
