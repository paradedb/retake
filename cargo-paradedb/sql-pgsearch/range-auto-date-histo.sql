SELECT CASE
    WHEN metrics_size < -10 THEN '(-∞, -10)'
    WHEN metrics_size >= -10 AND metrics_size < 10 THEN '[-10, 10)'
    WHEN metrics_size >= 10 AND metrics_size < 100 THEN '[10, 100)'
    WHEN metrics_size >= 100 AND metrics_size < 1000 THEN '[100, 1000)'
    WHEN metrics_size >= 1000 AND metrics_size < 2000 THEN '[1000, 2000)'
    WHEN metrics_size >= 2000 THEN '[2000, ∞)'
END AS size_range,
date_trunc('day', "timestamp") AS date_bucket,
COUNT(*) AS doc_count
FROM benchmark_eslogs
WHERE benchmark_eslogs @@@ 'id:>0'
GROUP BY 1, 2
ORDER BY 1, 2;
