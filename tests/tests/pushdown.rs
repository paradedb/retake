mod fixtures;

use fixtures::*;
use rstest::*;
use serde_json::Value;
use sqlx::PgConnection;

#[rstest]
fn pushdown(mut conn: PgConnection) {
    const OPERATORS: [&str; 6] = ["=", ">", "<", ">=", "<=", "<>"];
    const TYPES: &[[&str; 2]] = &[
        ["int2", "0"],
        ["int4", "0"],
        ["int8", "0"],
        ["float4", "0"],
        ["float8", "0"],
        ["date", "now()"],
        ["time", "now()"],
        ["timetz", "now()"],
        ["timestamp", "now()"],
        ["timestamptz", "now()"],
        ["text", "'foo'"],
        ["uuid", "gen_random_uuid()"],
    ];

    let sqlname = |sqltype: &str| -> String { String::from("col_") + &sqltype.replace('"', "") };

    let mut sql = String::new();
    sql += "CREATE TABLE test (id SERIAL8 NOT NULL PRIMARY KEY, col_boolean boolean DEFAULT false";
    for [sqltype, default] in TYPES {
        sql += &format!(
            ", {} {sqltype} NOT NULL DEFAULT {default}",
            sqlname(sqltype)
        );
    }
    sql += ");";

    eprintln!("{sql}");
    sql.execute(&mut conn);

    let sql = format!(
        r#"CREATE INDEX idxtest ON test USING bm25 (id, col_boolean, {}) WITH (key_field='id', text_fields = '{{"col_text": {{"tokenizer": {{"type":"raw"}} }} }}');"#,
        TYPES
            .iter()
            .map(|t| sqlname(t[0]))
            .collect::<Vec<_>>()
            .join(", ")
    );
    eprintln!("{sql}");
    sql.execute(&mut conn);

    "INSERT INTO test (id) VALUES (1);".execute(&mut conn); // insert all default values

    "SET enable_indexscan TO off;".execute(&mut conn);
    "SET enable_bitmapscan TO off;".execute(&mut conn);
    "SET max_parallel_workers TO 0;".execute(&mut conn);

    for operator in OPERATORS {
        for [sqltype, default] in TYPES {
            let sqlname = sqlname(sqltype);
            let sql = format!(
                r#"
                EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON)
                SELECT count(*)
                FROM test
                WHERE {sqlname} {operator} {default}::{sqltype}
                  AND id @@@ '1';
            "#
            );

            eprintln!("/----------/");
            eprintln!("{sql}");

            let (plan,) = sql.fetch_one::<(Value,)>(&mut conn);
            eprintln!("{plan:#?}");

            let plan = plan
                .pointer("/0/Plan/Plans/0")
                .unwrap()
                .as_object()
                .unwrap();
            pretty_assertions::assert_eq!(
                plan.get("Node Type"),
                Some(&Value::String(String::from("Custom Scan")))
            );
        }
    }

    // boolean is a bit of a separate beast, so test it directly
    {
        let sqltype = "boolean";
        let sqlname = sqlname(sqltype);
        let sql = format!(
            r#"
                EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON)
                SELECT count(*)
                FROM test
                WHERE {sqlname} = true
                  AND id @@@ '1';
            "#
        );

        eprintln!("/----------/");
        eprintln!("{sql}");

        let (plan,) = sql.fetch_one::<(Value,)>(&mut conn);
        eprintln!("{plan:#?}");

        let plan = plan
            .pointer("/0/Plan/Plans/0")
            .unwrap()
            .as_object()
            .unwrap();
        pretty_assertions::assert_eq!(
            plan.get("Node Type"),
            Some(&Value::String(String::from("Custom Scan")))
        );
    }
    {
        let sqltype = "boolean";
        let sqlname = sqlname(sqltype);
        let sql = format!(
            r#"
                EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON)
                SELECT count(*)
                FROM test
                WHERE {sqlname} = false
                  AND id @@@ '1';
            "#
        );

        eprintln!("/----------/");
        eprintln!("{sql}");

        let (plan,) = sql.fetch_one::<(Value,)>(&mut conn);
        eprintln!("{plan:#?}");

        let plan = plan
            .pointer("/0/Plan/Plans/0")
            .unwrap()
            .as_object()
            .unwrap();
        pretty_assertions::assert_eq!(
            plan.get("Node Type"),
            Some(&Value::String(String::from("Custom Scan")))
        );
    }
}
