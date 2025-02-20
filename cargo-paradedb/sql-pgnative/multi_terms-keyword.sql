SELECT
    process->>'name' AS process_name,
    cloud->>'region' AS cloud_region,
    COUNT(*) AS doc_count
FROM benchmark_eslogs
WHERE "timestamp" >= '2023-01-05T00:00:00Z'
  AND "timestamp" <  '2023-01-05T05:00:00Z'
GROUP BY 1, 2
ORDER BY doc_count DESC;

