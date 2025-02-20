SELECT *
FROM benchmark_eslogs
WHERE "timestamp" >= '2023-01-03T00:00:00Z'
  AND "timestamp" <  '2023-01-03T10:00:00Z'
  AND (
       message ILIKE '%monkey%'
    OR message ILIKE '%jackal%'
    OR message ILIKE '%bear%'
  );

