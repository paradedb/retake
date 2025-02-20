SELECT *
FROM benchmark_eslogs
WHERE process->>'name' = 'systemd'
  AND metrics_size >= 1
  AND metrics_size <= 100;

