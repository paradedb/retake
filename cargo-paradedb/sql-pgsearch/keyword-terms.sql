SELECT aws_cloudwatch->>'log_stream' AS log_stream, COUNT(*) AS doc_count
FROM benchmark_eslogs
WHERE benchmark_eslogs @@@ 'id:>0'
GROUP BY 1
ORDER BY doc_count DESC
LIMIT 500;
