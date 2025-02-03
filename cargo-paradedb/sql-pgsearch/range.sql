SELECT *
FROM benchmark_eslogs
WHERE benchmark_eslogs @@@ paradedb.range(
    field => 'timestamp',
    range => '[2023-01-01T00:00:00Z,2023-01-03T00:00:00Z)'::tstzrange
);
