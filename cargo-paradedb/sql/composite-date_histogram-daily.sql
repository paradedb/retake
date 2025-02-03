SELECT
    date_trunc('day', "timestamp") AS day_bucket,
    COUNT(*) AS doc_count
FROM benchmark_eslogs
WHERE "timestamp" >= '2022-12-30T00:00:00Z'
  AND "timestamp" <  '2023-01-07T12:00:00Z'
GROUP BY 1
ORDER BY 1;

