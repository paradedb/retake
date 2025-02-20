SELECT date_trunc('day', "timestamp") AS day_bucket,
       COUNT(*) AS doc_count
FROM benchmark_eslogs
WHERE benchmark_eslogs @@@ paradedb.range(
  field => 'timestamp',
  range => '[2022-12-30T00:00:00Z,2023-01-07T12:00:00Z)'::tstzrange
)
GROUP BY 1
ORDER BY 1;
