WITH top_streams AS (
  SELECT aws_cloudwatch->>'log_stream' AS log_stream
  FROM benchmark_eslogs
  WHERE benchmark_eslogs @@@ paradedb.range(
      field => 'timestamp',
      range => '[2023-01-01T00:00:00Z,2023-01-03T00:00:00Z)'::tstzrange
  )
  GROUP BY 1
  ORDER BY COUNT(*) DESC
  LIMIT 10
)
SELECT t.log_stream,
       b.process->>'name' AS process_name,
       COUNT(*) AS count_per_process
FROM benchmark_eslogs b
JOIN top_streams t
  ON b.aws_cloudwatch->>'log_stream' = t.log_stream
GROUP BY t.log_stream, process_name
ORDER BY t.log_stream, count_per_process DESC;
