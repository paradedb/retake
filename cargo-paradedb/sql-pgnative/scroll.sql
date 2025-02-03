-- In Postgres, "scroll" can be approximated by simple pagination:
SELECT *
FROM benchmark_eslogs
ORDER BY id;
-- For page-wise retrieval, e.g.:
-- SELECT * FROM benchmark_eslogs ORDER BY id LIMIT 1000 OFFSET <page*N>;

