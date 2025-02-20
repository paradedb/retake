SELECT *
FROM benchmark_eslogs
WHERE benchmark_eslogs @@@ paradedb.boolean(
    must => ARRAY[
        /* Must be within the timestamp range */
        paradedb.range(
            field => 'timestamp',
            range => '[2023-01-03T00:00:00Z,2023-01-03T10:00:00Z)'::tstzrange
        )
    ],
    /* At least one of these terms must match in 'message' */
    should => ARRAY[
        paradedb.term('message', 'monkey'),
        paradedb.term('message', 'jackal'),
        paradedb.term('message', 'bear')
    ]
);
