SELECT CASE WHEN metrics_size < 100 THEN '[0, 100)'
           WHEN metrics_size < 1000 THEN '[100, 1000)'
           ELSE '[2000, ∞)' END AS size_range,
       date_trunc('day', "timestamp") AS date_bucket,
       MIN((agent->>'tmin')::float) AS tmin,
       AVG(metrics_size) AS tavg,
       MAX(metrics_size) AS tmax
FROM benchmark_eslogs
WHERE benchmark_eslogs @@@ 'id:>0'
GROUP BY 1, 2
ORDER BY 1, 2;
