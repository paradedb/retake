SELECT *
FROM benchmark_eslogs
WHERE benchmark_eslogs @@@ paradedb.boolean(
  must => ARRAY[
    paradedb.range(
      field => 'timestamp',
      range => '[2023-01-03T00:00:00Z,2023-01-03T10:00:00Z)'::tstzrange
    )
  ],
  should => ARRAY[
    paradedb.term('message', 'monkey'),
    paradedb.term('message', 'jackal'),
    paradedb.term('message', 'bear')
  ]
)
ORDER BY "timestamp" ASC;
