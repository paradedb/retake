SELECT *
FROM benchmark_eslogs
WHERE (
    aws_cloudwatch->>'log_stream' = 'indigodagger'
    OR (metrics_size >= 10 AND metrics_size <= 20)
);

