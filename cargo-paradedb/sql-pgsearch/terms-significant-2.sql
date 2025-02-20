WITH top_process_names AS (
  SELECT process->>'name' AS proc_name
  FROM benchmark_eslogs
  WHERE benchmark_eslogs @@@ paradedb.range(
    field => 'timestamp',
    range => '[2023-01-01T00:00:00Z,2023-01-03T00:00:00Z)'::tstzrange
  )
  GROUP BY 1
  ORDER BY COUNT(*) DESC
  LIMIT 10
)
SELECT
  t.proc_name,
  b.aws_cloudwatch->>'log_stream' AS log_stream,
  COUNT(*) AS count_per_stream
FROM benchmark_eslogs b
JOIN top_process_names t ON b.process->>'name' = t.proc_name
GROUP BY t.proc_name, log_stream
ORDER BY t.proc_name, count_per_stream DESC;
