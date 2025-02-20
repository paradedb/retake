WITH pages AS (
  SELECT generate_series(0, 24) AS page
)
SELECT
  pages.page AS page_number,
  logs.*
FROM pages
CROSS JOIN LATERAL (
  SELECT *
  FROM benchmark_eslogs
  WHERE benchmark_eslogs @@@ paradedb.all()
  ORDER BY id
  LIMIT 1000
  OFFSET (pages.page * 1000)
) AS logs
ORDER BY pages.page, logs.id;
