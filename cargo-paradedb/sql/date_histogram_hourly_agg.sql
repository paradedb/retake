SELECT date_trunc('hour', "timestamp") AS hour_bucket,
       COUNT(*) AS doc_count
FROM benchmark_eslogs
GROUP BY 1
ORDER BY 1;

