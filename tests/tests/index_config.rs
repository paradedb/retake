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

#![allow(unused_variables, unused_imports)]
mod fixtures;

use std::path::PathBuf;

use fixtures::*;
use pretty_assertions::assert_eq;
use rstest::*;
use sqlx::PgConnection;

fn fmt_err<T: std::error::Error>(err: T) -> String {
    format!("unexpected error, received: {}", err)
}

#[rstest]
fn invalid_create_index(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'index_config', schema_name => 'public')"
        .execute(&mut conn);

    match r#"CREATE INDEX index_config_index ON index_config
        USING bm25 (id) "#
        .execute_result(&mut conn)
    {
        Ok(_) => panic!("should fail with no key_field"),
        Err(err) => assert_eq!(
            err.to_string(),
            "error returned from database: must specify key_field"
        ),
    };

    match r#"CREATE INDEX index_config_index ON index_config
        USING bm25 (id) WITH (key_field='id')"#
        .execute_result(&mut conn)
    {
        Ok(_) => panic!("should fail with no fields"),
        Err(err) => assert!(err.to_string().contains("specified"), "{}", fmt_err(err)),
    };
}

#[rstest]
fn prevent_duplicate(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'index_config', schema_name => 'paradedb')"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, description) WITH (key_field='id')"#
        .execute(&mut conn);

    match r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, description) WITH (key_field='id')"#
        .execute_result(&mut conn)
    {
        Ok(_) => panic!("should fail with relation already exists"),
        Err(err) => assert!(
            err.to_string().contains("already exists"),
            "{}",
            fmt_err(err)
        ),
    };
}

#[rstest]
async fn drop_column(mut conn: PgConnection) {
    r#"
    CREATE TABLE f_table (
        id SERIAL PRIMARY KEY,
        category TEXT
    );

    CREATE TABLE test_table (
        id SERIAL PRIMARY KEY,
        fkey INTEGER REFERENCES f_table ON UPDATE CASCADE ON DELETE RESTRICT,
        fulltext TEXT
    );

    INSERT INTO f_table (category) VALUES ('cat_a'), ('cat_b'), ('cat_c');
    INSERT INTO test_table (fkey, fulltext) VALUES (1, 'abc'), (1, 'def'), (2, 'ghi'), (3, 'jkl');
    "#
    .execute(&mut conn);

    r#"CREATE INDEX test_index ON test_table
        USING bm25 (id, fulltext) WITH (key_field='id')"#
        .execute(&mut conn);

    r#"DROP INDEX test_index CASCADE;
    ALTER TABLE test_table DROP COLUMN fkey;

    CREATE INDEX test_index ON test_table
        USING bm25 (id, fulltext) WITH (key_field='id')"#
        .execute(&mut conn);

    let rows: Vec<(String, String)> =
        "SELECT name, field_type FROM paradedb.schema('test_index')".fetch(&mut conn);

    assert_eq!(rows[0], ("ctid".into(), "U64".into()));
    assert_eq!(rows[1], ("fulltext".into(), "Str".into()));
    assert_eq!(rows[2], ("id".into(), "I64".into()));
}

#[rstest]
fn default_text_field(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'index_config', schema_name => 'paradedb')"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, description) WITH (key_field='id')"#
        .execute(&mut conn);

    let rows: Vec<(String, String)> =
        "SELECT name, field_type FROM paradedb.schema('paradedb.index_config_index')"
            .fetch(&mut conn);

    assert_eq!(rows[0], ("ctid".into(), "U64".into()));
    assert_eq!(rows[1], ("description".into(), "Str".into()));
    assert_eq!(rows[2], ("id".into(), "I64".into()));
}

#[rstest]
fn text_field_with_options(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'index_config', schema_name => 'paradedb')"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, description)
        WITH (key_field='id', text_fields='{"description": {"tokenizer": {"type": "en_stem", "normalizer": "raw"}, "record": "freq", "fast": true}}');
"#
        .execute(&mut conn);

    let rows: Vec<(String, String)> =
        "SELECT name, field_type FROM paradedb.schema('paradedb.index_config_index')"
            .fetch(&mut conn);

    assert_eq!(rows[0], ("ctid".into(), "U64".into()));
    assert_eq!(rows[1], ("description".into(), "Str".into()));
    assert_eq!(rows[2], ("id".into(), "I64".into()));
}

#[rstest]
fn multiple_text_fields(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'index_config', schema_name => 'paradedb')"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config

        USING bm25 (id, description, category)
        WITH (
            key_field='id',
            text_fields='{"description": {"tokenizer": {"type": "en_stem", "normalizer": "raw"}, "record": "freq", "fast": true}}'
        );
        "#
        .execute(&mut conn);

    let rows: Vec<(String, String)> =
        "SELECT name, field_type FROM paradedb.schema('paradedb.index_config_index')"
            .fetch(&mut conn);

    assert_eq!(rows[0], ("category".into(), "Str".into()));
    assert_eq!(rows[1], ("ctid".into(), "U64".into()));
    assert_eq!(rows[2], ("description".into(), "Str".into()));
    assert_eq!(rows[3], ("id".into(), "I64".into()));
}

#[rstest]
fn default_numeric_field(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'index_config', schema_name => 'paradedb')"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, rating) WITH (key_field='id')"#
        .execute(&mut conn);

    let rows: Vec<(String, String)> =
        "SELECT name, field_type FROM paradedb.schema('paradedb.index_config_index')"
            .fetch(&mut conn);

    assert_eq!(rows[0], ("ctid".into(), "U64".into()));
    assert_eq!(rows[1], ("id".into(), "I64".into()));
    assert_eq!(rows[2], ("rating".into(), "I64".into()));
}

#[rstest]
fn numeric_field_with_options(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'index_config', schema_name => 'paradedb')"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, rating) WITH (key_field='id', numeric_fields='{"rating": {"fast": true}}')"#
        .execute(&mut conn);

    let rows: Vec<(String, String)> =
        "SELECT name, field_type FROM paradedb.schema('paradedb.index_config_index')"
            .fetch(&mut conn);

    assert_eq!(rows[0], ("ctid".into(), "U64".into()));
    assert_eq!(rows[1], ("id".into(), "I64".into()));
    assert_eq!(rows[2], ("rating".into(), "I64".into()));
}

#[rstest]
fn default_boolean_field(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'index_config', schema_name => 'paradedb')"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, in_stock) WITH (key_field='id')"#
        .execute(&mut conn);

    let rows: Vec<(String, String)> =
        "SELECT name, field_type FROM paradedb.schema('paradedb.index_config_index')"
            .fetch(&mut conn);

    assert_eq!(rows[0], ("ctid".into(), "U64".into()));
    assert_eq!(rows[1], ("id".into(), "I64".into()));
    assert_eq!(rows[2], ("in_stock".into(), "Bool".into()));
}

#[rstest]
fn boolean_field_with_options(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'index_config', schema_name => 'paradedb')"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, in_stock) WITH (key_field='id', boolean_fields='{"in_stock": {"fast": false}}')"#
        .execute(&mut conn);

    let rows: Vec<(String, String)> =
        "SELECT name, field_type FROM paradedb.schema('paradedb.index_config_index')"
            .fetch(&mut conn);

    assert_eq!(rows[0], ("ctid".into(), "U64".into()));
    assert_eq!(rows[1], ("id".into(), "I64".into()));
    assert_eq!(rows[2], ("in_stock".into(), "Bool".into()));
}

#[rstest]
fn default_json_field(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'index_config', schema_name => 'paradedb')"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, metadata) WITH (key_field='id')"#
        .execute(&mut conn);

    let rows: Vec<(String, String)> =
        "SELECT name, field_type FROM paradedb.schema('paradedb.index_config_index')"
            .fetch(&mut conn);

    assert_eq!(rows[0], ("ctid".into(), "U64".into()));
    assert_eq!(rows[1], ("id".into(), "I64".into()));
    assert_eq!(rows[2], ("metadata".into(), "JsonObject".into()));
}

#[rstest]
fn json_field_with_options(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'index_config', schema_name => 'paradedb')"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, metadata)
        WITH (
            key_field='id',
            json_fields='{"metadata": {"fast": true, "expand_dots": false, "tokenizer": {"type": "raw", "normalizer": "raw"}}}'
        )"#
        .execute(&mut conn);

    let rows: Vec<(String, String)> =
        "SELECT name, field_type FROM paradedb.schema('paradedb.index_config_index')"
            .fetch(&mut conn);

    assert_eq!(rows[0], ("ctid".into(), "U64".into()));
    assert_eq!(rows[1], ("id".into(), "I64".into()));
    assert_eq!(rows[2], ("metadata".into(), "JsonObject".into()));
}

#[rstest]
fn default_datetime_field(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'index_config', schema_name => 'paradedb')"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, created_at, last_updated_date) WITH (key_field='id')"#
        .execute(&mut conn);

    let rows: Vec<(String, String)> =
        "SELECT name, field_type FROM paradedb.schema('paradedb.index_config_index')"
            .fetch(&mut conn);

    assert_eq!(rows[0], ("created_at".into(), "Date".into()));
    assert_eq!(rows[1], ("ctid".into(), "U64".into()));
    assert_eq!(rows[2], ("id".into(), "I64".into()));
    assert_eq!(rows[3], ("last_updated_date".into(), "Date".into()));
}

#[rstest]
fn datetime_field_with_options(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'index_config', schema_name => 'paradedb')"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, created_at, last_updated_date)
        WITH (key_field='id', datetime_fields='{"created_at": {"fast": true}, "last_updated_date": {"fast": false}}')"#
        .execute(&mut conn);

    let rows: Vec<(String, String)> =
        "SELECT name, field_type FROM paradedb.schema('paradedb.index_config_index')"
            .fetch(&mut conn);

    assert_eq!(rows[0], ("created_at".into(), "Date".into()));
    assert_eq!(rows[1], ("ctid".into(), "U64".into()));
    assert_eq!(rows[2], ("id".into(), "I64".into()));
    assert_eq!(rows[3], ("last_updated_date".into(), "Date".into()));
}

#[rstest]
fn multiple_fields(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'index_config', schema_name => 'paradedb')"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, description, category, rating, in_stock, metadata) WITH (key_field='id')"#
        .execute(&mut conn);

    let rows: Vec<(String, String)> =
        "SELECT name, field_type FROM paradedb.schema('paradedb.index_config_index')"
            .fetch(&mut conn);

    assert_eq!(rows[0], ("category".into(), "Str".into()));
    assert_eq!(rows[1], ("ctid".into(), "U64".into()));
    assert_eq!(rows[2], ("description".into(), "Str".into()));
    assert_eq!(rows[3], ("id".into(), "I64".into()));
    assert_eq!(rows[4], ("in_stock".into(), "Bool".into()));
    assert_eq!(rows[5], ("metadata".into(), "JsonObject".into()));
    assert_eq!(rows[6], ("rating".into(), "I64".into()));
}

#[rstest]
fn missing_schema_index(mut conn: PgConnection) {
    match "SELECT paradedb.schema('paradedb.missing_bm25_index')".fetch_result::<(i64,)>(&mut conn)
    {
        Err(err) => assert!(err
            .to_string()
            .contains(r#"relation "paradedb.missing_bm25_index" does not exist"#)),
        _ => panic!("non-existing index should throw an error"),
    }
}

#[rstest]
fn null_values(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'index_config', schema_name => 'paradedb')"
        .execute(&mut conn);

    "INSERT INTO paradedb.index_config (description, category, rating) VALUES ('Null Item 1', NULL, NULL), ('Null Item 2', NULL, 2)"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, description, category, rating, in_stock, metadata) WITH (key_field='id')"#
        .execute(&mut conn);

    let rows: Vec<(String, Option<String>, Option<i32>)> = "
        SELECT description, category, rating
        FROM paradedb.index_config WHERE index_config @@@ 'description:\"Null Item\"'
        ORDER BY id"
        .fetch(&mut conn);

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("Null Item 1".into(), None, None));
    assert_eq!(rows[1], ("Null Item 2".into(), None, Some(2)));

    let rows: Vec<(bool,)> =
        "SELECT in_stock FROM paradedb.index_config WHERE index_config @@@ 'in_stock:false'"
            .fetch(&mut conn);

    assert_eq!(rows.len(), 13);
}

#[rstest]
fn null_key_field_build(mut conn: PgConnection) {
    "CREATE TABLE paradedb.index_config(id INTEGER, description TEXT)".execute(&mut conn);
    "INSERT INTO paradedb.index_config VALUES (NULL, 'Null Item 1'), (2, 'Null Item 2')"
        .execute(&mut conn);

    match r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, description) WITH (key_field='id')"#
        .execute_result(&mut conn)
    {
        Ok(_) => panic!("should fail with null key_field"),
        Err(err) => assert_eq!(
            err.to_string(),
            "error returned from database: error creating index entries for index 'index_config_index': key_field column 'id' cannot be NULL"
        ),
    };
}

#[rstest]
fn null_key_field_insert(mut conn: PgConnection) {
    "CREATE TABLE paradedb.index_config(id INTEGER, description TEXT)".execute(&mut conn);
    "INSERT INTO paradedb.index_config VALUES (1, 'Null Item 1'), (2, 'Null Item 2')"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 (id, description) WITH (key_field='id')"#
        .execute(&mut conn);

    match "INSERT INTO paradedb.index_config VALUES (NULL, 'Null Item 3')".execute_result(&mut conn)
    {
        Ok(_) => panic!("should fail with null key_field"),
        Err(err) => assert_eq!(
            err.to_string(),
            "error returned from database: error creating index entries for index 'index_config_index': key_field column 'id' cannot be NULL"
        ),
    };
}

#[rstest]
fn column_name_camelcase(mut conn: PgConnection) {
    "CREATE TABLE paradedb.index_config(\"IdName\" INTEGER, \"ColumnName\" TEXT)"
        .execute(&mut conn);
    "INSERT INTO paradedb.index_config VALUES (1, 'Plastic Keyboard'), (2, 'Bluetooth Headphones')"
        .execute(&mut conn);

    r#"CREATE INDEX index_config_index ON paradedb.index_config
        USING bm25 ("IdName", "ColumnName") WITH (key_field='IdName')"#
        .execute(&mut conn);

    let rows: Vec<(i32, String)> =
        "SELECT * FROM paradedb.index_config WHERE index_config @@@ 'ColumnName:keyboard'"
            .fetch(&mut conn);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "Plastic Keyboard".into()));
}

#[rstest]
fn multi_index_insert_in_transaction(mut conn: PgConnection) {
    "CREATE TABLE paradedb.index_config1(id INTEGER, description TEXT)".execute(&mut conn);
    "CREATE TABLE paradedb.index_config2(id INTEGER, description TEXT)".execute(&mut conn);
    r#"CREATE INDEX index_config1_index ON paradedb.index_config1
        USING bm25 (id, description) WITH (key_field='id')"#
        .execute(&mut conn);
    r#"CREATE INDEX index_config2_index ON paradedb.index_config2
        USING bm25 (id, description) WITH (key_field='id')"#
        .execute(&mut conn);
    "BEGIN".execute(&mut conn);
    "INSERT INTO paradedb.index_config1 VALUES (1, 'Item 1'), (2, 'Item 2')".execute(&mut conn);
    "INSERT INTO paradedb.index_config2 VALUES (1, 'Item 1'), (2, 'Item 2')".execute(&mut conn);
    "COMMIT".execute(&mut conn);

    let rows: Vec<(i32, String)> =
        "SELECT * FROM paradedb.index_config1 WHERE index_config1 @@@ 'description:item'"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 2);

    let rows: Vec<(i32, String)> =
        "SELECT * FROM paradedb.index_config2 WHERE index_config2 @@@ 'description:item'"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 2);
}

#[rstest]
fn partitioned_index(mut conn: PgConnection) {
    // Set up the partitioned table with two partitions
    r#"
        CREATE TABLE sales (
            id SERIAL,
            sale_date DATE NOT NULL,
            amount real NOT NULL, description TEXT,
            PRIMARY KEY (id, sale_date)
        ) PARTITION BY RANGE (sale_date);

        CREATE TABLE sales_2023_q1 PARTITION OF sales
        FOR VALUES FROM ('2023-01-01') TO ('2023-03-31');

        CREATE TABLE sales_2023_q2 PARTITION OF sales
        FOR VALUES FROM ('2023-04-01') TO ('2023-06-30');

        INSERT INTO sales (sale_date, amount, description) VALUES
        ('2023-01-10', 150.00, 'Ergonomic metal keyboard'),
        ('2023-01-15', 200.00, 'Plastic keyboard'),
        ('2023-02-05', 300.00, 'Sleek running shoes'),
        ('2023-03-12', 175.50, 'Bluetooth speaker'),
        ('2023-03-25', 225.75, 'Artistic ceramic vase');

        INSERT INTO sales (sale_date, amount, description) VALUES
        ('2023-04-01', 250.00, 'Modern wall clock'),
        ('2023-04-18', 180.00, 'Designer wall paintings'),
        ('2023-05-09', 320.00, 'Handcrafted wooden frame');
    "#
    .execute(&mut conn);

    // Create the BM25 index on the partitioned table
    r#"CREATE INDEX sales_index ON sales_2023_q1
        USING bm25 (id, description, sale_date, amount) WITH (key_field='id', numeric_fields='{"amount": {"fast": true}}')
    "#
        .execute(&mut conn);

    // Test: Verify data is partitioned correctly by querying each partition
    let rows_q1: Vec<(i32, String, String)> = r#"
        SELECT id, description, sale_date::text FROM sales_2023_q1
    "#
    .fetch(&mut conn);
    assert_eq!(rows_q1.len(), 5, "Expected 5 rows in Q1 partition");

    let rows_q2: Vec<(i32, String, String)> = r#"
        SELECT id, description, sale_date::text FROM sales_2023_q2
    "#
    .fetch(&mut conn);
    assert_eq!(rows_q2.len(), 3, "Expected 3 rows in Q2 partition");

    // Test: Search using the bm25 index
    let search_results: Vec<(i32, String)> = r#"
        SELECT id, description FROM sales_2023_q1 WHERE id @@@ 'description:keyboard'
    "#
    .fetch(&mut conn);
    assert_eq!(search_results.len(), 2, "Expected 2 items with 'keyboard'");

    // Test: Retrieve items by a numeric range (amount field) and verify bm25 compatibility
    let amount_results: Vec<(i32, String, f32)> = r#"
        SELECT id, description, amount FROM sales_2023_q1
        WHERE amount @@@ '[175 TO 250]'
        ORDER BY amount ASC
    "#
    .fetch(&mut conn);
    assert_eq!(
        amount_results.len(),
        3,
        "Expected 3 items with amount in range 175-250"
    );
}

#[rstest]
fn custom_enum_term(mut conn: PgConnection) {
    r#"
    CREATE TYPE color AS ENUM ('red', 'green', 'blue');
    CREATE TABLE paradedb.index_config(id INTEGER, description TEXT, color color);
    INSERT INTO paradedb.index_config VALUES (1, 'Item 1', 'red'), (2, 'Item 2', 'green');
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX index_config_index ON paradedb.index_config
    USING bm25 (id, description, color)
    WITH (key_field='id');
    "#
    .execute(&mut conn);

    let rows: Vec<(i32, String)> =
        "SELECT id, description FROM paradedb.index_config WHERE id @@@ paradedb.term('color', 'red'::color)".fetch(&mut conn);

    assert_eq!(rows, vec![(1, "Item 1".into())]);
}

#[rstest]
fn custom_enum_parse(mut conn: PgConnection) {
    r#"
    CREATE TYPE color AS ENUM ('red', 'green', 'blue');
    CREATE TABLE paradedb.index_config(id INTEGER, description TEXT, color color);
    INSERT INTO paradedb.index_config VALUES (1, 'Item 1', 'red'), (2, 'Item 2', 'green');
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX index_config_index ON paradedb.index_config
    USING bm25 (id, description, color)
    WITH (key_field='id');
    "#
    .execute(&mut conn);

    let rows: Vec<(i32, String)> =
        "SELECT id, description FROM paradedb.index_config WHERE id @@@ paradedb.parse('color:1.0')".fetch(&mut conn);

    assert_eq!(rows, vec![(1, "Item 1".into())]);
}

#[rstest]
fn domain_types(mut conn: PgConnection) {
    // Create domain types for all of the built-in types pg_search supports.
    r#"
    CREATE DOMAIN nonemptytext AS text CHECK (VALUE <> '');
    CREATE DOMAIN nonemptyvarchar AS varchar CHECK (VALUE <> '');
    CREATE DOMAIN nonemptyuuid AS uuid CHECK (VALUE IS NOT NULL);
    CREATE DOMAIN possmallint AS smallint CHECK (VALUE > 0);
    CREATE DOMAIN posint AS integer CHECK (VALUE > 0);
    CREATE DOMAIN posbigint AS bigint CHECK (VALUE > 0);
    CREATE DOMAIN posreal AS real CHECK (VALUE > 0);
    CREATE DOMAIN posdouble AS double precision CHECK (VALUE > 0);
    CREATE DOMAIN posnumeric AS numeric CHECK (VALUE > 0);
    CREATE DOMAIN trueboolean AS boolean CHECK (VALUE = TRUE);
    CREATE DOMAIN nonemptyjsonbarray AS jsonb CHECK (jsonb_typeof(VALUE) = 'array' AND jsonb_array_length(VALUE) > 0);
    CREATE DOMAIN nonemptyjsonarray AS json CHECK (json_typeof(VALUE) = 'array' AND json_array_length(VALUE) > 0);
    CREATE DOMAIN posint4range AS int4range CHECK (lower(VALUE) > 0);
    CREATE DOMAIN posint8range AS int8range CHECK (lower(VALUE) > 0);
    CREATE DOMAIN posnumrange as numrange CHECK (lower(VALUE) > 0.0);
    CREATE DOMAIN daterange2025 as daterange CHECK (date_part('year', lower(VALUE)) = 2025 and date_part('year', upper(VALUE)) = 2025);
    CREATE DOMAIN tsrange2025 as tsrange CHECK (date_part('year', lower(VALUE)) = 2025 and date_part('year', upper(VALUE)) = 2025);
    CREATE DOMAIN tstzrange2025 as tstzrange CHECK (date_part('year', lower(VALUE)) = 2025 and date_part('year', upper(VALUE)) = 2025);
    CREATE DOMAIN date2025 as date CHECK (date_part('year', VALUE) = 2025 and date_part('year', VALUE) = 2025);
    CREATE DOMAIN ts2025 as timestamp CHECK (date_part('year', VALUE) = 2025 and date_part('year', VALUE) = 2025);
    CREATE DOMAIN tstz2025 as timestamptz CHECK (date_part('year', VALUE) = 2025 and date_part('year', VALUE) = 2025);
    CREATE DOMAIN noon as time CHECK (date_part('hour', VALUE) = 12);
    CREATE DOMAIN noontz as timetz CHECK (date_part('hour', VALUE) = 12);
    "#
    .execute(&mut conn);

    // Create a table containing all of the domain types in its schema and index them all.
    r#"
    CREATE TABLE paradedb.index_config(
        id INTEGER,
        n1 nonemptytext,
        n2 nonemptyvarchar,
        n3 nonemptyuuid,
        n4 possmallint,
        n5 posint,
        n6 posbigint,
        n7 posreal,
        n8 posdouble,
        n9 posnumeric,
        n10 trueboolean,
        n11 nonemptyjsonbarray,
        n12 nonemptyjsonarray,
        n13 posint4range,
        n14 posint8range,
        n15 posnumrange,
        n16 daterange2025,
        n17 tsrange2025,
        n18 tstzrange2025,
        n19 date2025,
        n20 ts2025,
        n21 tstz2025,
        n22 noon,
        n23 noontz
    );

    INSERT INTO paradedb.index_config VALUES
    (1, 'Item 1', 'Item 1', '11111111-1111-1111-1111-111111111111', 1, 1, 1, 1, 1, 1, TRUE, '[1, 2, 3]'::json, '[1, 2, 3]'::jsonb, '[1, 3]'::int4range, '[1, 3]'::int8range, '[1.0, 3.0]'::numrange, '[2025-01-01, 2025-01-31]'::daterange, '[2025-01-01, 2025-01-31]'::tsrange, '[2025-01-01, 2025-01-31]'::tstzrange, '2025-01-01'::date, '2025-01-01'::timestamp, '2025-01-01'::timestamptz, '12:01:00'::time, '12:01:00'::timetz),
    (2, 'Item 2', 'Item 2', '22222222-2222-2222-2222-222222222222', 2, 2, 2, 2, 2, 2, TRUE, '[2, 3, 4]'::json, '[2, 3, 4]'::jsonb, '[2, 4]'::int4range, '[2, 4]'::int8range, '[2.0, 4.0]'::numrange, '[2025-02-01, 2025-02-28]'::daterange, '[2025-02-01, 2025-02-28]'::tsrange, '[2025-02-01, 2025-02-28]'::tstzrange, '2025-02-01'::date, '2025-02-01'::timestamp, '2025-02-01'::timestamptz, '12:02:00'::time, '12:02:00'::timetz),
    (3, 'Item 3', 'Item 3', '33333333-3333-3333-3333-333333333333', 3, 3, 3, 3, 3, 3, TRUE, '[3, 4, 5]'::json, '[3, 4, 5]'::jsonb, '[3, 5]'::int4range, '[3, 5]'::int8range, '[3.0, 5.0]'::numrange, '[2025-03-01, 2025-03-31]'::daterange, '[2025-03-01, 2025-03-31]'::tsrange, '[2025-03-01, 2025-03-31]'::tstzrange, '2025-03-01'::date, '2025-03-01'::timestamp, '2025-03-01'::timestamptz, '12:03:00'::time, '12:03:00'::timetz);

    CREATE INDEX index_config_index ON paradedb.index_config
    USING bm25 (id, n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15, n16, n17, n18, n19, n20, n21, n22, n23)
    WITH (key_field='id');
    "#
    .execute(&mut conn);

    // Ensure all domain type values are indexed and queryable as expected.
    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n1 @@@ 'Item 2' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(2,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n2 @@@ 'Item 2' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(2,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n3 @@@ '22222222' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(2,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n4 @@@ '2' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(2,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n5 @@@ '2' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(2,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n6 @@@ '2' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(2,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n7 @@@ '2' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(2,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n8 @@@ '2' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(2,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n9 @@@ '2' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(2,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n10 @@@ 'true' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(1,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n11 @@@ '5' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(3,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n12 @@@ '5' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(3,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE id @@@ paradedb.range_term('n13', 5) ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(3,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE id @@@ paradedb.range_term('n14', 5) ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(3,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE id @@@ paradedb.range_term('n15', 5.0) ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(3,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE id @@@ paradedb.range_term('n16', '2025-03-01'::date) ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(3,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE id @@@ paradedb.range_term('n17', '2025-03-01'::timestamp) ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(3,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE id @@@ paradedb.range_term('n18', '2025-03-01'::timestamptz) ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(3,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n19 @@@ '\"2025-03-01T00:00:00Z\"' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(3,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n20 @@@ '\"2025-03-01T00:00:00Z\"' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(3,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n21 @@@ '\"2025-03-01T00:00:00Z\"' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(3,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n22 @@@ '\"1970-01-01T12:03:00Z\"' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(3,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM paradedb.index_config WHERE n23 @@@ '\"1970-01-01T12:03:00Z\"' ORDER BY paradedb.score(id) DESC LIMIT 1".fetch(&mut conn);
    assert_eq!(rows, vec![(3,)]);
}
