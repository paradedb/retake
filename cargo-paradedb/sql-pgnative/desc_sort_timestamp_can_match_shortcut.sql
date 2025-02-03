SELECT *
FROM benchmark_eslogs
WHERE process->>'name' = 'kernel'
ORDER BY "timestamp" DESC;

