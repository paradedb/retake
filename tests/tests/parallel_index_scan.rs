// Copyright (c) 2023-2024 Retake, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

mod fixtures;

use fixtures::db::Query;
use fixtures::*;
use rstest::*;
use serde_json::Value;
use sqlx::PgConnection;

#[rstest]
fn index_scan_under_parallel_path(mut conn: PgConnection) {
    let (major_version,) = r#"select (regexp_match(version(), 'PostgreSQL (\d+)'))[1]::int;"#
        .fetch_one::<(i32,)>(&mut conn);
    if major_version < 16 {
        // the `debug_parallel_query` was added in pg16, so we simply cannot run this test on anything
        // less than pg16
        return;
    }

    SimpleProductsTable::setup().execute(&mut conn);

    r#"
        set paradedb.enable_custom_scan to off;
        set enable_indexonlyscan to off;
        set debug_parallel_query to on;
    "#
    .execute(&mut conn);

    let count = r#"
        select count(1) from paradedb.bm25_search where description @@@ 'shoes';
    "#
    .fetch::<(i64,)>(&mut conn);
    assert_eq!(count, vec![(3,)]);
}

#[rstest]
fn dont_do_parallel_index_scan(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    "VACUUM paradedb.bm25_search".execute(&mut conn);
    "set enable_indexscan to off;".execute(&mut conn);
    let (plan, ) = "EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) select count(*) from paradedb.bm25_search where description @@@ 'shoes';".fetch_one::<(Value,)>(&mut conn);
    let plan = plan
        .pointer("/0/Plan/Plans/0/Plans/0/Plans/0")
        .unwrap()
        .as_object()
        .unwrap();
    eprintln!("{plan:#?}");
    pretty_assertions::assert_eq!(
        plan.get("Node Type"),
        Some(&Value::String(String::from("Custom Scan")))
    );
    pretty_assertions::assert_eq!(
        plan.get("Virtual Tuples"),
        Some(&Value::Number(serde_json::Number::from(3)))
    );

    let count = r#"
        select count(*) from paradedb.bm25_search where description @@@ 'shoes';
    "#
    .fetch::<(i64,)>(&mut conn);
    assert_eq!(count, vec![(3,)]);
}
