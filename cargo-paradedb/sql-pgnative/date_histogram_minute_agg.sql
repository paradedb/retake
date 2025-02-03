SELECT date_trunc('minute', "timestamp") AS minute_bucket,
       COUNT(*) AS doc_count
FROM benchmark_eslogs
WHERE "timestamp" >= '2023-01-01T00:00:00Z'
  AND "timestamp" <  '2023-01-03T00:00:00Z'
GROUP BY 1
ORDER BY 1;

