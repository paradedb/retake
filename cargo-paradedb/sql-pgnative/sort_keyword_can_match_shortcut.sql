SELECT *
FROM benchmark_eslogs
WHERE process->>'name' = 'kernel'
ORDER BY log_file_path ASC;

