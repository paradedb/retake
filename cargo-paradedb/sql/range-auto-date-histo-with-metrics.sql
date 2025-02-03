-- Approximation in SQL: group metrics_size into ranges, then date_trunc, then show min/avg/max.
-- 'metrics.tmin' is not explicitly in the schema, so we might guess it is in JSON (agent->>'tmin'), etc.

SELECT
    CASE
        WHEN metrics_size < 100 THEN '[0, 100)'
        WHEN metrics_size < 1000 THEN '[100, 1000)'
        WHEN metrics_size < 2000 THEN '[1000, 2000)'
        ELSE '[2000, ∞)'
    END AS size_range,
    date_trunc('day', "timestamp") AS date_bucket,
    MIN((agent->>'tmin')::float) AS tmin,  -- example parse from JSON
    AVG(metrics_size)              AS tavg,
    MAX(metrics_size)              AS tmax
FROM benchmark_eslogs
GROUP BY 1, 2
ORDER BY 1, 2;

