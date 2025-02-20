SELECT *
FROM benchmark_eslogs
WHERE log_file_path = '/var/log/messages/solarshark'
ORDER BY metrics_size DESC;

