SELECT *
FROM benchmark_eslogs
WHERE message ILIKE '%monkey%'
   OR message ILIKE '%jackal%'
   OR message ILIKE '%bear%';

