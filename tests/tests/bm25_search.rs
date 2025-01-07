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

use anyhow::Result;
use approx::assert_relative_eq;
use core::panic;
use fixtures::*;
use pgvector::Vector;
use pretty_assertions::assert_eq;
use rstest::*;
use sqlx::{types::BigDecimal, PgConnection};
use std::str::FromStr;

#[rstest]
async fn basic_search_query(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    SimpleProductsTable::setup().execute(&mut conn);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'description:keyboard OR category:electronics' ORDER BY id"
            .fetch_collect(&mut conn);

    assert_eq!(
        columns.description,
        concat!(
            "Ergonomic metal keyboard,Plastic Keyboard,Innovative wireless earbuds,",
            "Fast charging power bank,Bluetooth-enabled speaker"
        )
        .split(',')
        .collect::<Vec<_>>()
    );

    assert_eq!(
        columns.category,
        "Electronics,Electronics,Electronics,Electronics,Electronics"
            .split(',')
            .collect::<Vec<_>>()
    );

    Ok(())
}

#[rstest]
async fn basic_search_ids(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'description:keyboard OR category:electronics' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![1, 2, 12, 22, 32]);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'description:keyboard' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![1, 2]);
}

#[rstest]
fn json_search(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'metadata.color:white' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![4, 15, 25]);
}

#[rstest]
fn date_search(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'last_updated_date:[2023-04-15T00:00:00Z TO 2023-04-18T00:00:00Z]' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![2, 23, 41]);
}

#[rstest]
fn timestamp_search(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'created_at:[2023-04-15T00:00:00Z TO 2023-04-18T00:00:00Z]' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![2, 22, 23, 41]);
}

#[rstest]
fn real_time_search(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    "INSERT INTO paradedb.bm25_search (description, rating, category, in_stock, metadata, created_at, last_updated_date, latest_available_time)
        VALUES ('New keyboard', 5, 'Electronics', true, '{}', TIMESTAMP '2023-05-04 11:09:12', DATE '2023-05-06', TIME '10:07:10')"
        .execute(&mut conn);
    "DELETE FROM paradedb.bm25_search WHERE id = 1".execute(&mut conn);
    "UPDATE paradedb.bm25_search SET description = 'PVC Keyboard' WHERE id = 2".execute(&mut conn);

    let columns: SimpleProductsTableVec = "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'description:keyboard OR category:electronics' ORDER BY id"
        .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![2, 12, 22, 32, 42]);
}

#[rstest]
fn sequential_scan_syntax(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let columns: SimpleProductsTableVec = "SELECT * FROM paradedb.bm25_search
        WHERE paradedb.search_with_query_input(
            id,
            paradedb.parse('category:electronics')
        ) ORDER BY id"
        .to_string()
        .fetch_collect(&mut conn);

    assert_eq!(columns.id, vec![1, 2, 12, 22, 32]);
}

#[rstest]
fn quoted_table_name(mut conn: PgConnection) {
    r#"CREATE TABLE "Activity" (key SERIAL, name TEXT, age INTEGER);
    INSERT INTO "Activity" (name, age) VALUES ('Alice', 29);
    INSERT INTO "Activity" (name, age) VALUES ('Bob', 34);
    INSERT INTO "Activity" (name, age) VALUES ('Charlie', 45);
    INSERT INTO "Activity" (name, age) VALUES ('Diana', 27);
    INSERT INTO "Activity" (name, age) VALUES ('Fiona', 38);
    INSERT INTO "Activity" (name, age) VALUES ('George', 41);
    INSERT INTO "Activity" (name, age) VALUES ('Hannah', 22);
    INSERT INTO "Activity" (name, age) VALUES ('Ivan', 30);
    INSERT INTO "Activity" (name, age) VALUES ('Julia', 25);
    CREATE INDEX activity ON "Activity"
    USING bm25 ("key", name) WITH (key_field='key')"#
        .execute(&mut conn);
    let row: (i32, String, i32) =
        "SELECT * FROM \"Activity\" WHERE \"Activity\" @@@ 'name:alice' ORDER BY key"
            .fetch_one(&mut conn);

    assert_eq!(row, (1, "Alice".into(), 29));
}

#[rstest]
fn text_arrays(mut conn: PgConnection) {
    r#"CREATE TABLE example_table (
        id SERIAL PRIMARY KEY,
        text_array TEXT[],
        varchar_array VARCHAR[]
    );
    INSERT INTO example_table (text_array, varchar_array) VALUES 
    ('{"text1", "text2", "text3"}', '{"vtext1", "vtext2"}'),
    ('{"another", "array", "of", "texts"}', '{"vtext3", "vtext4", "vtext5"}'),
    ('{"single element"}', '{"single varchar element"}');
    CREATE INDEX example_table_idx ON public.example_table
    USING bm25 (id, text_array, varchar_array)
    WITH (
        key_field = 'id',
        text_fields = '{
            "text_array": {},
            "varchar_array": {}
        }'
    );"#
    .execute(&mut conn);
    let row: (i32,) =
        r#"SELECT * FROM example_table WHERE example_table @@@ 'text_array:text1' ORDER BY id"#
            .fetch_one(&mut conn);

    assert_eq!(row, (1,));

    let row: (i32,) =
        r#"SELECT * FROM example_table WHERE example_table @@@ 'text_array:"single element"' ORDER BY id"#.fetch_one(&mut conn);

    assert_eq!(row, (3,));

    let rows: Vec<(i32,)> =
        r#"SELECT * FROM example_table WHERE example_table @@@ 'varchar_array:varchar OR text_array:array' ORDER BY id"#
            .fetch(&mut conn);

    assert_eq!(rows[0], (2,));
    assert_eq!(rows[1], (3,));
}

#[rstest]
fn int_arrays(mut conn: PgConnection) {
    r#"CREATE TABLE example_table (
        id SERIAL PRIMARY KEY,
        int_array INT[],
        bigint_array BIGINT[]
    );
    INSERT INTO example_table (int_array, bigint_array) VALUES 
    ('{1, 2, 3}', '{100, 200}'),
    ('{4, 5, 6}', '{300, 400, 500}'),
    ('{7, 8, 9}', '{600, 700, 800, 900}');
    CREATE INDEX example_table_idx ON public.example_table
    USING bm25 (id, int_array, bigint_array)
    WITH (key_field = 'id');"#
        .execute(&mut conn);

    let rows: Vec<(i32,)> =
        "SELECT id FROM example_table WHERE example_table @@@ 'int_array:1' ORDER BY id"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1,));

    let rows: Vec<(i32,)> =
        "SELECT id FROM example_table WHERE example_table @@@ 'bigint_array:500' ORDER BY id"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (2,));
}

#[rstest]
fn boolean_arrays(mut conn: PgConnection) {
    r#"CREATE TABLE example_table (
        id SERIAL PRIMARY KEY,
        bool_array BOOLEAN[]
    );
    INSERT INTO example_table (bool_array) VALUES 
    ('{true, true, true}'),
    ('{false, false, false}'),
    ('{true, true, false}');

    CREATE INDEX example_table_idx ON example_table
    USING bm25 (id, bool_array) WITH (key_field='id')
    "#
    .execute(&mut conn);

    let rows: Vec<(i32,)> =
        "SELECT id FROM example_table WHERE example_table @@@ 'bool_array:true' ORDER BY id"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1,));
    assert_eq!(rows[1], (3,));

    let rows: Vec<(i32,)> =
        "SELECT id FROM example_table WHERE example_table @@@ 'bool_array:false' ORDER BY id"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (2,));
    assert_eq!(rows[1], (3,));
}

#[rstest]
fn datetime_arrays(mut conn: PgConnection) {
    r#"CREATE TABLE example_table (
        id SERIAL PRIMARY KEY,
        date_array DATE[],
        timestamp_array TIMESTAMP[]
    );
    INSERT INTO example_table (date_array, timestamp_array) VALUES 
    (ARRAY['2023-01-01'::DATE, '2023-02-01'::DATE], ARRAY['2023-02-01 12:00:00'::TIMESTAMP, '2023-02-01 13:00:00'::TIMESTAMP]),
    (ARRAY['2023-03-01'::DATE, '2023-04-01'::DATE], ARRAY['2023-04-01 14:00:00'::TIMESTAMP, '2023-04-01 15:00:00'::TIMESTAMP]),
    (ARRAY['2023-05-01'::DATE, '2023-06-01'::DATE], ARRAY['2023-06-01 16:00:00'::TIMESTAMP, '2023-06-01 17:00:00'::TIMESTAMP]);
    CREATE INDEX example_table_idx ON example_table
    USING bm25 (id, date_array, timestamp_array) WITH (key_field='id')
    "#.execute(&mut conn);

    let rows: Vec<(i32,)> =
        r#"SELECT id FROM example_table WHERE example_table @@@ 'date_array:"2023-02-01T00:00:00Z"' ORDER BY id"#
            .fetch(&mut conn);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1,));

    let rows: Vec<(i32,)> =
        r#"SELECT id FROM example_table WHERE example_table @@@ 'timestamp_array:"2023-04-01T15:00:00Z"' ORDER BY id"#
            .fetch(&mut conn);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (2,));
}

#[rstest]
fn json_arrays(mut conn: PgConnection) {
    r#"CREATE TABLE example_table (
        id SERIAL PRIMARY KEY,
        json_array JSONB[]
    );
    INSERT INTO example_table (json_array) VALUES 
    (ARRAY['{"name": "John", "age": 30}'::JSONB, '{"name": "Jane", "age": 25}'::JSONB]),
    (ARRAY['{"name": "Bob", "age": 40}'::JSONB, '{"name": "Alice", "age": 35}'::JSONB]),
    (ARRAY['{"name": "Mike", "age": 50}'::JSONB, '{"name": "Lisa", "age": 45}'::JSONB]);"#
        .execute(&mut conn);

    match "CREATE INDEX example_table_idx ON example_table USING bm25 (id, json_array) WITH (key_field='id')"
    .execute_result(&mut conn)
    {
        Ok(_) => panic!("json arrays should not yet be supported"),
        Err(err) => assert!(err.to_string().contains("not yet supported")),
    }
}

#[rstest]
fn uuid(mut conn: PgConnection) {
    r#"
    CREATE TABLE uuid_table (
        id SERIAL PRIMARY KEY,
        random_uuid UUID,
        some_text text
    );

    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('f159c89e-2162-48cd-85e3-e42b71d2ecd0', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('38bf27a0-1aa8-42cd-9cb0-993025e0b8d0', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('b5faacc0-9eba-441a-81f8-820b46a3b57e', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('eb833eb6-c598-4042-b84a-0045828fceea', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('ea1181a0-5d3e-4f5f-a6ab-b1354ffc91ad', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('28b6374a-67d3-41c8-93af-490712f9923e', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('f6e85626-298e-4112-9abb-3856f8aa046a', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('88345d21-7b89-4fd6-87e4-83a4f68dbc3c', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('40bc9216-66d0-4ae8-87ee-ddb02e3e1b33', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('02f9789d-4963-47d5-a189-d9c114f5cba4', 'some text');

    CREATE INDEX uuid_table_bm25_index ON uuid_table
    USING bm25 (id, some_text) WITH (key_field='id');   
    
    DROP INDEX uuid_table_bm25_index CASCADE;"#
        .execute(&mut conn);

    r#"
    CREATE INDEX uuid_table_bm25_index ON uuid_table
    USING bm25 (id, some_text, random_uuid) WITH (key_field='id')   
    "#
    .execute(&mut conn);

    let rows: Vec<(i32,)> =
        r#"SELECT * FROM uuid_table WHERE uuid_table @@@ 'some_text:some' ORDER BY id"#
            .fetch(&mut conn);

    assert_eq!(rows.len(), 10);
}

#[rstest]
fn multi_tree(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);
    let columns: SimpleProductsTableVec = r#"
    SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@
	paradedb.boolean(
	    should => ARRAY[
		    paradedb.parse('description:shoes'),
		    paradedb.phrase_prefix(field => 'description', phrases => ARRAY['book']),
		    paradedb.term(field => 'description', value => 'speaker'),
		    paradedb.fuzzy_term(field => 'description', value => 'wolo', transposition_cost_one => false, distance => 1, prefix => true)
	    ]
    ) ORDER BY id
    "#
    .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![3, 4, 5, 7, 10, 32, 33, 34, 37, 39, 41]);
}

#[rstest]
fn snippet(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);
    let row: (i32, String, f32) = "
        SELECT id, paradedb.snippet(description), paradedb.score(id)
        FROM paradedb.bm25_search WHERE bm25_search @@@ 'description:shoes' ORDER BY id"
        .fetch_one(&mut conn);

    assert_eq!(row.0, 3);
    assert_eq!(row.1, "Sleek running <b>shoes</b>");
    assert_relative_eq!(row.2, 2.484906, epsilon = 1e-6);

    let row: (i32, String, f32) = "
        SELECT id, paradedb.snippet(description, '<h1>', '</h1>'), paradedb.score(id)
        FROM paradedb.bm25_search WHERE bm25_search @@@ 'description:shoes' ORDER BY id"
        .fetch_one(&mut conn);

    assert_eq!(row.0, 3);
    assert_eq!(row.1, "Sleek running <h1>shoes</h1>");
    assert_relative_eq!(row.2, 2.484906, epsilon = 1e-6);

    let row: (i32, String, f32) = "
    SELECT id, paradedb.snippet(description, '<h1>', '</h1>', max_num_chars=>2), paradedb.score(id)
    FROM paradedb.bm25_search WHERE bm25_search @@@ 'description:shoes' ORDER BY id"
        .fetch_one(&mut conn);

    assert_eq!(row.0, 3);
    assert_eq!(row.1, "<h1>shoes</h1>");
    assert_relative_eq!(row.2, 2.484906, epsilon = 1e-6);
}

#[rstest]
fn hybrid_with_single_result(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );

    CREATE INDEX search_idx
    ON mock_items
    USING bm25 (id, description, category, rating, in_stock, metadata, created_at)
    WITH (
        key_field='id',
        text_fields='{"description": {}, "category": {}}',
        numeric_fields='{"rating": {}}',
        boolean_fields='{"in_stock": {}}',
        json_fields='{"metadata": {}}'
    );

    CREATE EXTENSION vector;
    ALTER TABLE mock_items ADD COLUMN embedding vector(3);

    UPDATE mock_items m
    SET embedding = ('[' ||
        ((m.id + 1) % 10 + 1)::integer || ',' ||
        ((m.id + 2) % 10 + 1)::integer || ',' ||
        ((m.id + 3) % 10 + 1)::integer || ']')::vector;
    "#
    .execute(&mut conn);

    // Here, we'll delete all rows in the table but the first.
    // This previously triggered a "division by zero" error when there was
    // only one result in the similarity query. This test ensures that we
    // check for that condition.
    "DELETE FROM mock_items WHERE id != 1".execute(&mut conn);

    let rows: Vec<(i32, BigDecimal, String, Vector)> = r#"
    WITH semantic_search AS (
        SELECT id, RANK () OVER (ORDER BY embedding <=> '[1,2,3]') AS rank
        FROM mock_items ORDER BY embedding <=> '[1,2,3]' LIMIT 20
    ),
    bm25_search AS (
        SELECT id, RANK () OVER (ORDER BY paradedb.score(id) DESC) as rank
        FROM mock_items WHERE description @@@ 'keyboard' LIMIT 20
    )
    SELECT
        COALESCE(semantic_search.id, bm25_search.id) AS id,
        COALESCE(1.0 / (60 + semantic_search.rank), 0.0) +
        COALESCE(1.0 / (60 + bm25_search.rank), 0.0) AS score,
        mock_items.description,
        mock_items.embedding
    FROM semantic_search
    FULL OUTER JOIN bm25_search ON semantic_search.id = bm25_search.id
    JOIN mock_items ON mock_items.id = COALESCE(semantic_search.id, bm25_search.id)
    ORDER BY score DESC, description
    LIMIT 5
    "#
    .fetch(&mut conn);
    assert_eq!(
        rows,
        vec![(
            1,
            BigDecimal::from_str("0.03278688524590163934").unwrap(),
            String::from("Ergonomic metal keyboard"),
            Vector::from(vec![3.0, 4.0, 5.0])
        ),]
    );
}

#[rstest]
fn update_non_indexed_column(mut conn: PgConnection) -> Result<()> {
    // Create the test table and index.
    "CALL paradedb.create_bm25_test_table(table_name => 'mock_items', schema_name => 'public');"
        .execute(&mut conn);

    // For this test, we'll turn off autovacuum, as we'll be measuring the size of the index.
    // We don't want a vacuum to happen and unexpectedly change the size.
    "ALTER TABLE mock_items SET (autovacuum_enabled = false)".execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (key_field='id', text_fields='{"description": {"tokenizer": {"type": "en_stem", "lowercase": true, "remove_long": 255}}}')        
    "#
      .execute(&mut conn);

    let page_size_before: (i64,) =
        "SELECT pg_relation_size('search_idx') / current_setting('block_size')::int AS page_count"
            .fetch_one(&mut conn);
    // Update a non-indexed column.
    "UPDATE mock_items set category = 'Books' WHERE description = 'Sleek running shoes'"
        .execute(&mut conn);

    let page_size_after: (i64,) =
        "SELECT pg_relation_size('search_idx') / current_setting('block_size')::int AS page_count"
            .fetch_one(&mut conn);
    // The total page count should not have changed when updating a non-indexed column.
    assert_eq!(page_size_before, page_size_after);

    Ok(())
}

#[rstest]
async fn json_array_flattening(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    // Insert a JSON array into the metadata field
    "INSERT INTO paradedb.bm25_search (description, category, rating, in_stock, metadata, created_at, last_updated_date) VALUES 
    ('Product with array', 'Electronics', 4, true, '{\"colors\": [\"red\", \"green\", \"blue\"]}', now(), current_date)"
        .execute(&mut conn);

    // Search for individual elements in the JSON array
    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'metadata.colors:red' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![42]);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'metadata.colors:green' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![42]);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'metadata.colors:blue' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![42]);
}

#[rstest]
async fn json_array_multiple_documents(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    // Insert multiple documents with JSON arrays
    "INSERT INTO paradedb.bm25_search (description, category, rating, in_stock, metadata, created_at, last_updated_date) VALUES 
    ('Product 1', 'Electronics', 5, true, '{\"colors\": [\"red\", \"green\"]}', now(), current_date),
    ('Product 2', 'Electronics', 3, false, '{\"colors\": [\"blue\", \"yellow\"]}', now(), current_date),
    ('Product 3', 'Electronics', 4, true, '{\"colors\": [\"green\", \"blue\"]}', now(), current_date)"
        .execute(&mut conn);

    // Search for individual elements and verify the correct documents are returned
    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'metadata.colors:red' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![42]);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'metadata.colors:green' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![42, 44]);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'metadata.colors:blue' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![43, 44]);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'metadata.colors:yellow' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![43]);
}

#[rstest]
async fn json_array_mixed_data(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    // Insert documents with mixed data types in JSON arrays
    "INSERT INTO paradedb.bm25_search (description, category, rating, in_stock, metadata, created_at, last_updated_date) VALUES 
    ('Product with mixed array', 'Electronics', 5, true, '{\"attributes\": [\"fast\", 4, true]}', now(), current_date)"
        .execute(&mut conn);

    // Search for each data type element in the JSON array
    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'metadata.attributes:fast' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![42]);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'metadata.attributes:4' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![42]);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'metadata.attributes:true' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![42]);
}

#[rstest]
async fn json_nested_arrays(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    // Insert a document with nested JSON arrays into the metadata field
    "INSERT INTO paradedb.bm25_search (description, category, rating, in_stock, metadata, created_at, last_updated_date) VALUES 
    ('Product with nested array', 'Electronics', 4, true, '{\"specs\": {\"dimensions\": [\"width\", [\"height\", \"depth\"]]}}', now(), current_date)"
        .execute(&mut conn);

    // Search for elements in the nested JSON arrays
    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'metadata.specs.dimensions:width' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![42]);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'metadata.specs.dimensions:height' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![42]);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'metadata.specs.dimensions:depth' ORDER BY id"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![42]);
}

#[rstest]
fn bm25_partial_index_search(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    "CALL paradedb.create_bm25_test_table(table_name => 'test_partial_index', schema_name => 'paradedb');".execute(&mut conn);

    let ret = r#"
    CREATE INDEX partial_idx ON paradedb.test_partial_index
    USING bm25 (id, description, category, rating)
    WITH (
        key_field = 'id',
        text_fields = '{
            "description": {
                "tokenizer": {"type": "en_stem"}
            }
        }'
    ) WHERE category = 'Electronics';
    "#
    .execute_result(&mut conn);
    assert!(ret.is_ok(), "{ret:?}");

    // Ensure returned rows match the predicate
    let columns: SimpleProductsTableVec =
        "SELECT * FROM paradedb.test_partial_index WHERE test_partial_index @@@ 'rating:>1' ORDER BY rating LIMIT 20"
            .fetch_collect(&mut conn);
    assert_eq!(columns.category.len(), 5);
    assert_eq!(
        columns.category,
        "Electronics,Electronics,Electronics,Electronics,Electronics"
            .split(',')
            .collect::<Vec<_>>()
    );
    assert_eq!(columns.rating, vec![3, 4, 4, 4, 5]);

    // Ensure no mismatch rows returned
    let rows: Vec<(String, String)> = "
    SELECT description, category FROM paradedb.test_partial_index
    WHERE test_partial_index @@@ '(description:jeans OR category:Footwear) AND rating:>1'
    ORDER BY rating LIMIT 20"
        .fetch(&mut conn);
    assert_eq!(rows.len(), 0);

    // Insert multiple tuples only 1 matches predicate and query
    "INSERT INTO paradedb.test_partial_index (description, category, rating, in_stock) VALUES 
    ('Product 1', 'Electronics', 2, true),
    ('Product 2', 'Electronics', 1, false),
    ('Product 3', 'Footwear', 2, true)"
        .execute(&mut conn);

    let rows: Vec<(String, i32, String)> = "
    SELECT description, rating, category FROM paradedb.test_partial_index
    WHERE test_partial_index @@@ 'rating:>1'
    ORDER BY rating LIMIT 20"
        .fetch(&mut conn);
    assert_eq!(rows.len(), 6);

    let (desc, rating, category) = rows[0].clone();
    assert_eq!(desc, "Product 1");
    assert_eq!(rating, 2);
    assert_eq!(category, "Electronics");

    // Update one tuple to make it no longer match the predicate
    "UPDATE paradedb.test_partial_index SET category = 'Footwear' WHERE description = 'Product 1'"
        .execute(&mut conn);

    let rows: Vec<(String, i32, String)> = "
    SELECT description, rating, category FROM paradedb.test_partial_index
    WHERE test_partial_index @@@ 'rating:>1'
    ORDER BY rating LIMIT 20"
        .fetch(&mut conn);
    assert_eq!(rows.len(), 5);
    let (desc, ..) = rows[0].clone();
    assert_ne!(desc, "Product 1");

    // Update one tuple to make it match the predicate
    "UPDATE paradedb.test_partial_index SET category = 'Electronics' WHERE description = 'Product 3'"
        .execute(&mut conn);

    let rows: Vec<(String, i32, String)> = "
    SELECT description, rating, category FROM paradedb.test_partial_index
    WHERE test_partial_index @@@ 'rating:>1'
    ORDER BY rating LIMIT 20"
        .fetch(&mut conn);
    assert_eq!(rows.len(), 6);

    let (desc, rating, category) = rows[0].clone();
    assert_eq!(desc, "Product 3");
    assert_eq!(rating, 2);
    assert_eq!(category, "Electronics");

    // Insert one row without specifying the column referenced by the predicate.
    let rows: Vec<(String, i32, String)> = "
    SELECT description, rating, category FROM paradedb.test_partial_index
    WHERE test_partial_index @@@ 'rating:>1'
    ORDER BY rating LIMIT 20"
        .fetch(&mut conn);
    assert_eq!(rows.len(), 6);
}

#[rstest]
fn bm25_partial_index_hybrid(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );

    CREATE EXTENSION vector;
    ALTER TABLE mock_items ADD COLUMN embedding vector(3);

    UPDATE mock_items m
    SET embedding = ('[' ||
        ((m.id + 1) % 10 + 1)::integer || ',' ||
        ((m.id + 2) % 10 + 1)::integer || ',' ||
        ((m.id + 3) % 10 + 1)::integer || ']')::vector;
    "#
    .execute(&mut conn);

    let ret = r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating)
    WITH (
        key_field='id',
        text_fields='{
            "description": {"tokenizer": {"type": "en_stem", "lowercase": true, "remove_long": 255}},
            "category": {}
        }',
        numeric_fields='{"rating": {}}'
    ) WHERE category = 'Electronics';"#
    .execute_result(&mut conn);
    assert!(ret.is_ok(), "{ret:?}");

    let rows: Vec<(i32, BigDecimal, String, String, Vector)> = r#"WITH semantic_search AS (
    SELECT id, RANK () OVER (ORDER BY embedding <=> '[1,2,3]') AS rank
        FROM mock_items
        ORDER BY embedding <=> '[1,2,3]' LIMIT 20
    ),
    bm25_search AS (
        SELECT id, RANK () OVER (ORDER BY paradedb.score(id) DESC) AS rank
        FROM mock_items
        WHERE mock_items @@@ 'rating:>1'
        AND category = 'Electronics'
        LIMIT 20
    )
    SELECT
        COALESCE(semantic_search.id, bm25_search.id) AS id,
        COALESCE(1.0 / (60 + semantic_search.rank), 0.0) +
        COALESCE(1.0 / (60 + bm25_search.rank), 0.0) AS score,
        mock_items.description,
        mock_items.category,
        mock_items.embedding
    FROM semantic_search
    JOIN bm25_search ON semantic_search.id = bm25_search.id
    JOIN mock_items ON mock_items.id = COALESCE(semantic_search.id, bm25_search.id)
    ORDER BY score DESC, description
    "#
    .fetch(&mut conn);

    assert_eq!(rows.len(), 5);
    assert_eq!(
        rows.iter().map(|r| r.3.clone()).collect::<Vec<_>>(),
        "Electronics,Electronics,Electronics,Electronics,Electronics"
            .split(',')
            .collect::<Vec<_>>()
    );

    "INSERT INTO mock_items (description, category, rating, in_stock) VALUES
    ('Product 1', 'Electronics', 2, true),
    ('Product 2', 'Electronics', 1, false),
    ('Product 3', 'Footwear', 2, true);

    UPDATE mock_items m
    SET embedding = ('[' ||
    ((m.id + 1) % 10 + 1)::integer || ',' ||
    ((m.id + 2) % 10 + 1)::integer || ',' ||
    ((m.id + 3) % 10 + 1)::integer || ']')::vector;"
        .execute(&mut conn);

    let rows: Vec<(i32, BigDecimal, String, String, Vector)> = r#"
    WITH semantic_search AS (
    SELECT id, RANK () OVER (ORDER BY embedding <=> '[1,2,3]') AS rank
        FROM mock_items
        ORDER BY embedding <=> '[1,2,3]' LIMIT 20
    ),
    bm25_search AS (
        SELECT id, RANK () OVER (ORDER BY paradedb.score(id) DESC) AS rank
        FROM mock_items
        WHERE mock_items @@@ 'rating:>1'
        AND category = 'Electronics'
        LIMIT 20
    )
    SELECT
        COALESCE(semantic_search.id, bm25_search.id) AS id,
        COALESCE(1.0 / (60 + semantic_search.rank), 0.0) +
        COALESCE(1.0 / (60 + bm25_search.rank), 0.0) AS score,
        mock_items.description,
        mock_items.category,
        mock_items.embedding
    FROM semantic_search
    JOIN bm25_search ON semantic_search.id = bm25_search.id
    JOIN mock_items ON mock_items.id = COALESCE(semantic_search.id, bm25_search.id)
    ORDER BY score DESC, description
    "#
    .fetch(&mut conn);

    assert_eq!(rows.len(), 6);
    assert_eq!(
        rows.iter().map(|r| r.3.clone()).collect::<Vec<_>>(),
        "Electronics,Electronics,Electronics,Electronics,Electronics,Electronics"
            .split(',')
            .collect::<Vec<_>>()
    )
}

#[rstest]
fn bm25_partial_index_invalid_statement(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    "CALL paradedb.create_bm25_test_table(table_name => 'test_partial_index', schema_name => 'paradedb');".execute(&mut conn);

    // Ensure report error when predicate is invalid
    // unknown column
    let ret = r#"
    CREATE INDEX partial_idx ON paradedb.test_partial_index
    USING bm25 (id, description, category, rating)
    WITH (
        key_field = 'id',
        text_fields = '{
            "description": {
                "tokenizer": {"type": "en_stem"}
            }
        }'
    ) WHERE city = 'Electronics';
    "#
    .execute_result(&mut conn);
    assert!(ret.is_err());

    // mismatch type
    let ret = r#"
    CREATE INDEX partial_idx ON paradedb.test_partial_index
    USING bm25 (id, description, category, rating)
    WITH (
        key_field = 'id',
        text_fields = '{
            "description": {
                "tokenizer": {"type": "en_stem"}
            }
        }'
    ) WHERE city = 'Electronics';
    "#
    .execute_result(&mut conn);
    assert!(ret.is_err());

    let ret = r#"
    CREATE INDEX partial_idx ON paradedb.test_partial_index
    USING bm25 (id, description, category, rating)
    WITH (
        key_field = 'id',
        text_fields = '{
            "description": {
                "tokenizer": {"type": "en_stem"}
            }
        }'
    ) WHERE category = 'Electronics';
    "#
    .execute_result(&mut conn);
    assert!(ret.is_ok(), "{ret:?}");
}

#[rstest]
fn bm25_partial_index_alter_and_drop(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    "CALL paradedb.create_bm25_test_table(table_name => 'test_partial_index', schema_name => 'paradedb');".execute(&mut conn);

    r#"
    CREATE INDEX partial_idx ON paradedb.test_partial_index 
    USING bm25 (id, description, category, rating)
    WITH (
        key_field='id',
        text_fields='{
            "description": {"tokenizer": {"type": "en_stem", "lowercase": true, "remove_long": 255}}
        }'
    ) WHERE category = 'Electronics';
    "#
    .execute(&mut conn);
    let rows: Vec<(String,)> =
        "SELECT relname FROM pg_class WHERE relname = 'partial_idx';".fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    // Drop a column that is not referenced in the partial index.
    "ALTER TABLE paradedb.test_partial_index DROP COLUMN metadata;".execute(&mut conn);
    let rows: Vec<(String,)> =
        "SELECT relname FROM pg_class WHERE relname = 'partial_idx';".fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    // When the predicate column is dropped with CASCADE, the index and the corresponding
    // schema are both dropped.
    "ALTER TABLE paradedb.test_partial_index DROP COLUMN category CASCADE;".execute(&mut conn);
    let rows: Vec<(String,)> =
        "SELECT relname FROM pg_class WHERE relname = 'partial_idx';".fetch(&mut conn);
    assert_eq!(rows.len(), 0);

    r#"
    CREATE INDEX partial_idx ON paradedb.test_partial_index 
    USING bm25 (id, description, rating)
    WITH (
        key_field='id',
        text_fields='{
            "description": {"tokenizer": {"type": "en_stem", "lowercase": true, "remove_long": 255}}
        }'
    );
    "#
    .execute(&mut conn);

    let rows: Vec<(String,)> =
        "SELECT relname FROM pg_class WHERE relname = 'partial_idx';".fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    "DROP INDEX paradedb.partial_idx".execute(&mut conn);

    let rows: Vec<(String,)> =
        "SELECT relname FROM pg_class WHERE relname = 'partial_idx'".fetch(&mut conn);
    assert_eq!(rows.len(), 0);
}

#[rstest]
fn high_limit_rows(mut conn: PgConnection) {
    "CREATE TABLE large_series (id SERIAL PRIMARY KEY, description TEXT);".execute(&mut conn);
    "INSERT INTO large_series (description) SELECT 'Product ' || i FROM generate_series(1, 200000) i;"
        .execute(&mut conn);

    r#"
    CREATE INDEX large_series_idx ON public.large_series
    USING bm25 (id, description)
    WITH (key_field = 'id');
    "#
    .execute(&mut conn);

    let rows: Vec<(i32,)> =
        "SELECT id FROM large_series WHERE large_series @@@ 'description:Product' ORDER BY id"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 200000);
}

#[rstest]
fn json_term(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let rows: Vec<(i32,)> = "
        SELECT id FROM paradedb.bm25_search 
        WHERE paradedb.bm25_search.id @@@ paradedb.term('metadata.color', 'white') 
        ORDER BY id
    "
    .fetch(&mut conn);
    assert_eq!(rows, vec![(4,), (15,), (25,)]);

    r#"
    UPDATE paradedb.bm25_search
    SET metadata = '{"attributes": {"score": 4, "keywords": ["electronics", "headphones"]}}'::jsonb
    WHERE id = 1
    "#
    .execute(&mut conn);

    let rows: Vec<(i32,)> = "
    SELECT id FROM paradedb.bm25_search
    WHERE paradedb.bm25_search.id @@@ paradedb.term('metadata.attributes.score', 4)
    ORDER BY id
    "
    .fetch(&mut conn);
    assert_eq!(rows, vec![(1,)]);

    let rows: Vec<(i32,)> = "
    SELECT id FROM paradedb.bm25_search
    WHERE paradedb.bm25_search.id @@@ paradedb.term('metadata.attributes.keywords', 'electronics')
    ORDER BY id
    "
    .fetch(&mut conn);
    assert_eq!(rows, vec![(1,)]);
}

#[rstest]
fn json_fuzzy_term(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let rows: Vec<(i32,)> = "
        SELECT id FROM paradedb.bm25_search 
        WHERE paradedb.bm25_search.id @@@ paradedb.fuzzy_term('metadata.color', 'whiet') 
        ORDER BY id
    "
    .fetch(&mut conn);
    assert_eq!(rows, vec![(4,), (15,), (25,)]);
}

#[rstest]
fn json_phrase(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);
    r#"
    UPDATE paradedb.bm25_search 
    SET metadata = '{"attributes": {"review": "really good quality product"}}'::jsonb 
    WHERE id = 1
    "#
    .execute(&mut conn);

    let rows: Vec<(i32,)> = "
    SELECT id FROM paradedb.bm25_search 
    WHERE paradedb.bm25_search.id @@@ paradedb.phrase('metadata.attributes.review', ARRAY['good', 'quality']) 
    ORDER BY id
    "
    .fetch(&mut conn);
    assert_eq!(rows, vec![(1,)]);

    let rows: Vec<(i32,)> = "
    SELECT id FROM paradedb.bm25_search 
    WHERE paradedb.bm25_search.id @@@ paradedb.phrase('metadata.attributes.review', ARRAY['good', 'product'], slop => 1) 
    ORDER BY id
    "
    .fetch(&mut conn);
    assert_eq!(rows, vec![(1,)]);
}

#[rstest]
fn json_phrase_prefix(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);
    r#"
    UPDATE paradedb.bm25_search 
    SET metadata = '{"attributes": {"review": "really good quality product"}}'::jsonb 
    WHERE id = 1
    "#
    .execute(&mut conn);

    let rows: Vec<(i32,)> = "
    SELECT id FROM paradedb.bm25_search 
    WHERE paradedb.bm25_search.id @@@ paradedb.phrase_prefix('metadata.attributes.review', ARRAY['really', 'go']) 
    ORDER BY id
    "
    .fetch(&mut conn);
    assert_eq!(rows, vec![(1,)]);
}

#[rstest]
fn json_fuzzy_phrase(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);
    r#"
    UPDATE paradedb.bm25_search 
    SET metadata = '{"attributes": {"review": "really good quality product"}}'::jsonb 
    WHERE id = 1
    "#
    .execute(&mut conn);

    let rows: Vec<(i32,)> = "
    SELECT id FROM paradedb.bm25_search 
    WHERE paradedb.bm25_search.id @@@ paradedb.fuzzy_phrase('metadata.attributes.review', 'realy godo') 
    ORDER BY id
    "
    .fetch(&mut conn);
    assert_eq!(rows, vec![(1,)]);
}

#[rstest]
fn json_range(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(table_name => 'bm25_search', schema_name => 'paradedb');"
        .execute(&mut conn);
    "CREATE INDEX bm25_search_idx ON paradedb.bm25_search
    USING bm25 (id, metadata)
    WITH (
        key_field='id',
        json_fields='{\"metadata\": {\"fast\": true}}'
    )"
    .execute(&mut conn);

    r#"
    UPDATE paradedb.bm25_search
    SET metadata = '{"attributes": {"score": 3, "tstz": "2023-05-01T08:12:34Z"}}'::jsonb 
    WHERE id = 1;

    UPDATE paradedb.bm25_search
    SET metadata = '{"attributes": {"score": 4, "tstz": "2023-05-01T09:12:34Z"}}'::jsonb 
    WHERE id = 2;
    "#
    .execute(&mut conn);

    let rows: Vec<(i32,)> = "
    SELECT id FROM paradedb.bm25_search 
    WHERE paradedb.bm25_search.id @@@ paradedb.range('metadata.attributes.score', int4range(3, NULL, '[)')) 
    ORDER BY id
    "
    .fetch(&mut conn);
    assert_eq!(rows, vec![(1,), (2,)]);

    let rows: Vec<(i32,)> = "
    SELECT id FROM paradedb.bm25_search 
    WHERE paradedb.bm25_search.id @@@ paradedb.range('metadata.attributes.score', int4range(4, NULL, '[)')) 
    ORDER BY id
    "
    .fetch(&mut conn);
    assert_eq!(rows, vec![(2,)]);

    let rows: Vec<(i32,)> = "
    SELECT id FROM paradedb.bm25_search
    WHERE paradedb.bm25_search.id @@@ paradedb.range('metadata.attributes.tstz', tstzrange('2023-05-01T09:12:00Z', NULL, '[)'))
    ORDER BY id
    "
    .fetch(&mut conn);
    assert_eq!(rows, vec![(2,)]);
}

#[rstest]
fn test_customers_table(mut conn: PgConnection) {
    "CALL paradedb.create_bm25_test_table(
        table_name => 'customers',
        schema_name => 'public',
        table_type => 'Customers'
    );"
    .execute(&mut conn);

    r#"CREATE INDEX customers_idx ON customers 
    USING bm25 (id, name, crm_data)
    WITH (
        key_field='id',
        text_fields='{"name": {}}',
        json_fields='{"crm_data": {}}'
    );"#
    .execute(&mut conn);

    // Test querying by name
    let rows: Vec<(i32,)> =
        "SELECT id FROM customers WHERE customers @@@ 'name:Deep' ORDER BY id".fetch(&mut conn);
    assert_eq!(rows, vec![(2,)]);

    // Test querying nested JSON data
    let rows: Vec<(i32,)> = "SELECT id FROM customers WHERE customers @@@ 'crm_data.level1.level2.level3:deep_value' ORDER BY id"
        .fetch(&mut conn);
    assert_eq!(rows, vec![(2,)]);
}

#[rstest]
fn json_array_term(mut conn: PgConnection) {
    r#"
    CREATE TABLE colors (id SERIAL PRIMARY KEY, colors_json JSON, colors_jsonb JSONB);
    INSERT INTO colors (colors_json, colors_jsonb) VALUES 
        ('["red", "green", "blue"]'::JSON, '["red", "green", "blue"]'::JSONB),
        ('["red", "orange"]'::JSON, '["red", "orange"]'::JSONB);
    CREATE INDEX colors_bm25_index ON colors
    USING bm25 (id, colors_json, colors_jsonb)
    WITH (
        key_field='id',
        json_fields='{"colors_json": {}, "colors_jsonb": {}}'
    );
    "#
    .execute(&mut conn);

    let rows: Vec<(i32,)> = "
        SELECT id FROM colors 
        WHERE colors.id @@@ paradedb.term('colors_json', 'red') 
        ORDER BY id"
        .fetch(&mut conn);
    assert_eq!(rows, vec![(1,), (2,)]);

    let rows: Vec<(i32,)> = "
        SELECT id FROM colors 
        WHERE colors.id @@@ paradedb.term('colors_jsonb', 'red') 
        ORDER BY id"
        .fetch(&mut conn);
    assert_eq!(rows, vec![(1,), (2,)]);

    let rows: Vec<(i32,)> = "
        SELECT id FROM colors 
        WHERE colors.id @@@ paradedb.term('colors_json', 'green') 
        ORDER BY id"
        .fetch(&mut conn);
    assert_eq!(rows, vec![(1,)]);

    let rows: Vec<(i32,)> = "
        SELECT id FROM colors 
        WHERE colors.id @@@ paradedb.term('colors_jsonb', 'green') 
        ORDER BY id"
        .fetch(&mut conn);
    assert_eq!(rows, vec![(1,)]);
}

#[rstest]
fn multiple_tokenizers_with_alias(mut conn: PgConnection) {
    // Create the table
    "CREATE TABLE products (
        id SERIAL PRIMARY KEY,
        name TEXT,
        description TEXT
    );"
    .execute(&mut conn);

    // Insert mock data
    "INSERT INTO products (name, description) VALUES
    ('Mechanical Keyboard', 'RGB backlit keyboard with Cherry MX switches'),
    ('Wireless Mouse', 'Ergonomic mouse with long battery life'),
    ('4K Monitor', 'Ultra-wide curved display with HDR'),
    ('Gaming Laptop', 'Powerful laptop with dedicated GPU'),
    ('Ergonomic Chair', 'Adjustable office chair with lumbar support'),
    ('Standing Desk', 'Electric height-adjustable desk'),
    ('Noise-Cancelling Headphones', 'Over-ear headphones with active noise cancellation'),
    ('Mechanical Pencil', 'Precision drafting tool with 0.5mm lead'),
    ('Wireless Keyboard', 'Slim keyboard with multi-device support'),
    ('Graphic Tablet', 'Digital drawing pad with pressure sensitivity'),
    ('Curved Monitor', 'Immersive gaming display with high refresh rate'),
    ('Ergonomic Keyboard', 'Split design keyboard for comfortable typing'),
    ('Vertical Mouse', 'Upright mouse design to reduce wrist strain'),
    ('Ultrabook Laptop', 'Thin and light laptop with all-day battery'),
    ('LED Desk Lamp', 'Adjustable lighting with multiple color temperatures');"
        .execute(&mut conn);

    // Create the BM25 index
    r#"CREATE INDEX products_index ON products
    USING bm25 (id, name, description)
    WITH (
        key_field='id',
        text_fields='{
            "name": {
                "tokenizer": {"type": "default"}
            },
            "name_stem": {
                "source": "name",
                "tokenizer": {"type": "default", "stemmer": "English"},
                "column": "name"
            },
            "description": {
                "tokenizer": {"type": "default"}
            },
            "description_stem": {
                "source": "description", 
                "tokenizer": {"type": "default", "stemmer": "English"},
                "column": "description"
            }
        }'
    );"#
    .execute(&mut conn);

    // Test querying with default tokenizer
    let rows: Vec<(i32, String)> =
        "SELECT id, name FROM products WHERE id @@@ paradedb.parse('name:Keyboard')"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().any(|(_, name)| name == "Mechanical Keyboard"));
    assert!(rows.iter().any(|(_, name)| name == "Wireless Keyboard"));
    assert!(rows.iter().any(|(_, name)| name == "Ergonomic Keyboard"));

    // Ensure that the default tokenizer doesn't return for stemmed queries
    let rows: Vec<(i32, String)> =
        "SELECT id, name FROM products WHERE id @@@ paradedb.parse('name:Keyboards')"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 0);

    // Test querying with stemmed alias
    let rows: Vec<(i32, String)> =
        "SELECT id, name FROM products WHERE id @@@ paradedb.parse('name_stem:Keyboards')"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().any(|(_, name)| name == "Mechanical Keyboard"));
    assert!(rows.iter().any(|(_, name)| name == "Wireless Keyboard"));
    assert!(rows.iter().any(|(_, name)| name == "Ergonomic Keyboard"));

    // Test querying description with default tokenizer
    let rows: Vec<(i32, String)> =
        "SELECT id, name FROM products WHERE id @@@ paradedb.parse('description:battery')"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|(_, name)| name == "Wireless Mouse"));
    assert!(rows.iter().any(|(_, name)| name == "Ultrabook Laptop"));

    // Ensure that the default tokenizer doesn't return for stemmed queries
    let rows: Vec<(i32, String)> =
        "SELECT id, name FROM products WHERE id @@@ paradedb.parse('description:displaying')"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 0);

    // Test querying description with stemmed alias
    let rows: Vec<(i32, String)> =
        "SELECT id, name FROM products WHERE id @@@ paradedb.parse('description_stem:displaying')"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|(_, name)| name == "4K Monitor"));
    assert!(rows.iter().any(|(_, name)| name == "Curved Monitor"));

    // Test querying with both default and stemmed fields
    let rows: Vec<(i32, String)> =
        "SELECT id, name FROM products WHERE id @@@ paradedb.parse('name:Mouse OR description_stem:mouses')"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|(_, name)| name == "Wireless Mouse"));
    assert!(rows.iter().any(|(_, name)| name == "Vertical Mouse"));
}

#[rstest]
fn alias_cannot_be_key_field(mut conn: PgConnection) {
    // Create the table
    "CREATE TABLE products (
        id SERIAL PRIMARY KEY,
        name TEXT,
        description TEXT
    );
        INSERT INTO products (name, description) VALUES 
        ('apple', 'fruit'),
        ('banana', 'fruit'), 
        ('cherry', 'fruit'), 
        ('banana split', 'fruit');
    "
    .execute(&mut conn);

    // Test alias cannot be the same as key_field
    let result = r#"
    CREATE INDEX products_index ON products
    USING bm25 (id, name, description)
    WITH (
        key_field='id',
        text_fields='{
            "name": {
                "tokenizer": {"type": "default"}
            },
            "id": {
                "tokenizer": {"type": "default", "stemmer": "English"},
                "column": "description"
            }
        }'
    );"#
    .execute_result(&mut conn);

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "error returned from database: cannot override BM25 configuration for key_field 'id', you must use an aliased field name and 'column' configuration key"
    );

    // Test valid configuration where alias is different from key_field
    r#"
    CREATE INDEX products_index ON products
    USING bm25 (id, name, description)
    WITH (
        key_field='id',
        text_fields='{
            "name": {
                "tokenizer": {"type": "default"}
            }
        }',
        numeric_fields='{
            "id_aliased": {
                "column": "id"
            }
        }'
    );"#
    .execute(&mut conn);

    let rows: Vec<(i32,)> =
        "SELECT id FROM products WHERE id @@@ paradedb.parse('id_aliased:1')".fetch(&mut conn);

    assert_eq!(rows, vec![(1,)])
}

#[rstest]
fn multiple_tokenizers_same_field_in_query(mut conn: PgConnection) {
    // Create the table
    "CREATE TABLE product_reviews (
        id SERIAL PRIMARY KEY,
        product_name TEXT,
        review_text TEXT
    );"
    .execute(&mut conn);

    // Insert mock data
    "INSERT INTO product_reviews (product_name, review_text) VALUES
    ('SmartPhone X', 'This smartphone is incredible! The camera quality is amazing.'),
    ('Laptop Pro', 'Great laptop for programming. The keyboard is comfortable.'),
    ('Wireless Earbuds', 'These earbuds have excellent sound quality. Battery life could be better.'),
    ('Gaming Mouse', 'Responsive and comfortable. Perfect for long gaming sessions.'),
    ('4K TV', 'The picture quality is breathtaking. Smart features work seamlessly.'),
    ('Fitness Tracker', 'Accurate step counting and heart rate monitoring. The app is user-friendly.'),
    ('Smartwatch', 'This watch is smart indeed! Great for notifications and fitness tracking.'),
    ('Bluetooth Speaker', 'Impressive sound for its size. Waterproof feature is a plus.'),
    ('Mechanical Keyboard', 'Satisfying key presses. RGB lighting is customizable.'),
    ('External SSD', 'Super fast read/write speeds. Compact and portable design.');"
    .execute(&mut conn);

    // Create the BM25 index with multiple tokenizers
    r#"CREATE INDEX product_reviews_index ON product_reviews
    USING bm25 (id, product_name, review_text)
    WITH (
        key_field='id',
        text_fields='{
            "product_name": {
                "tokenizer": {"type": "default"}
            },
            "product_name_ngram": {
                "column": "product_name",
                "tokenizer": {"type": "ngram", "min_gram": 3, "max_gram": 3, "prefix_only": false}
            },
            "review_text": {
                "tokenizer": {"type": "default"}
            },
            "review_text_stem": {
                "column": "review_text",
                "tokenizer": {"type": "default", "stemmer": "English"}
            }
        }'
    );"#
    .execute(&mut conn);

    //  Exact match using default tokenizer
    let rows: Vec<(i32, String)> = r#"SELECT id, product_name FROM product_reviews WHERE id @@@ paradedb.parse('product_name:"Wireless Earbuds"')"#
        .fetch(&mut conn);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, "Wireless Earbuds");

    // Partial match using ngram tokenizer
    let rows: Vec<(i32, String)> =
        "SELECT id, product_name FROM product_reviews WHERE id @@@ paradedb.parse('product_name_ngram:phon')"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, "SmartPhone X");

    // Stemmed search using English stemmer tokenizer
    let rows: Vec<(i32, String)> =
        "SELECT id, product_name FROM product_reviews WHERE id @@@ paradedb.parse('review_text_stem:gaming')"
            .fetch(&mut conn);
    assert_eq!(rows.len(), 1);
    assert!(rows.iter().any(|(_, name)| name == "Gaming Mouse"));

    // Using default tokenizer and stem on same field
    let rows: Vec<(i32, String)> = "SELECT id, product_name FROM product_reviews WHERE id @@@ paradedb.parse('review_text:monitoring OR review_text_stem:mon')"
        .fetch(&mut conn);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, "Fitness Tracker");
}

#[rstest]
fn more_like_this_with_alias(mut conn: PgConnection) {
    // Create the table
    r#"
    CREATE TABLE test_more_like_this_alias (
        id SERIAL PRIMARY KEY,
        flavour TEXT,
        description TEXT
    );

    INSERT INTO test_more_like_this_alias (flavour, description) VALUES 
        ('apple', 'A sweet and crisp fruit'),
        ('banana', 'A long yellow tropical fruit'),
        ('cherry', 'A small round red fruit'),
        ('banana split', 'An ice cream dessert with bananas'),
        ('apple pie', 'A dessert made with apples');
    "#
    .execute(&mut conn);

    // Create the BM25 index with aliased fields
    r#"
    CREATE INDEX test_more_like_this_alias_index ON test_more_like_this_alias
    USING bm25 (id, flavour, description)
    WITH (
        key_field='id',
        text_fields='{
            "taste": {
                "column": "flavour",
                "tokenizer": {"type": "default"}
            },
            "details": {
                "column": "description",
                "tokenizer": {"type": "default"}
            }
        }'
    );
    "#
    .execute(&mut conn);

    // Test more_like_this with aliased field 'taste' (original 'flavour')
    let rows: Vec<(i32, String, String)> = r#"
    SELECT id, flavour, description FROM test_more_like_this_alias 
    WHERE id @@@ paradedb.more_like_this(
        min_doc_frequency => 0,
        min_term_frequency => 0,
        document_fields => '{"taste": "banana"}'
    );
    "#
    .fetch_collect(&mut conn);

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|(_, flavour, _)| flavour == "banana"));
    assert!(rows.iter().any(|(_, flavour, _)| flavour == "banana split"));
}

#[rstest]
fn multiple_aliases_same_column(mut conn: PgConnection) {
    // Test multiple aliases pointing to the same column with different tokenizers
    "CREATE TABLE multi_alias (
        id SERIAL PRIMARY KEY,
        content TEXT
    );"
    .execute(&mut conn);

    "INSERT INTO multi_alias (content) VALUES 
    ('running and jumping'),
    ('ran and jumped'),
    ('runner jumper athlete');"
        .execute(&mut conn);

    // Create index with multiple aliases for same column
    r#"CREATE INDEX multi_alias_idx ON multi_alias
    USING bm25 (id, content)
    WITH (
        key_field='id',
        text_fields='{
            "content": {
                "tokenizer": {"type": "default"}
            },
            "content_stem": {
                "column": "content",
                "tokenizer": {"type": "default", "stemmer": "English"}
            },
            "content_ngram": {
                "column": "content", 
                "tokenizer": {"type": "ngram", "min_gram": 3, "max_gram": 3, "prefix_only": false}
            }
        }'
    );"#
    .execute(&mut conn);

    // Test each alias configuration
    let rows: Vec<(i32,)> =
        "SELECT id FROM multi_alias WHERE multi_alias @@@ 'content:running'".fetch(&mut conn);
    assert_eq!(rows, vec![(1,)]);

    let rows: Vec<(i32,)> =
        "SELECT id FROM multi_alias WHERE multi_alias @@@ 'content_stem:running'".fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(i32,)> =
        "SELECT id FROM multi_alias WHERE multi_alias @@@ 'content_ngram:run'".fetch(&mut conn);
    assert_eq!(rows.len(), 2);
}

#[rstest]
fn missing_source_column(mut conn: PgConnection) {
    "CREATE TABLE missing_source (
        id SERIAL PRIMARY KEY,
        text_field TEXT
    );"
    .execute(&mut conn);

    // Attempt to create index with alias pointing to non-existent column
    let result = r#"CREATE INDEX missing_source_idx ON missing_source
    USING bm25 (id, text_field)
    WITH (
        key_field='id',
        text_fields='{
            "alias": {
                "column": "nonexistent_column",
                "tokenizer": {"type": "default"}
            }
        }'
    );"#
    .execute_result(&mut conn);

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "error returned from database: 'nonexistent_column' cannot be indexed as a text field"
    );
}

#[rstest]
fn alias_type_mismatch(mut conn: PgConnection) {
    "CREATE TABLE type_mismatch (
        id SERIAL PRIMARY KEY,
        numeric_field INTEGER,
        text_field TEXT
    );"
    .execute(&mut conn);

    // Try to create text alias pointing to numeric column
    let result = r#"CREATE INDEX type_mismatch_idx ON type_mismatch
    USING bm25 (id, numeric_field, text_field)
    WITH (
        key_field='id',
        text_fields='{
            "wrong_type": {
                "column": "numeric_field",
                "tokenizer": {"type": "default"}
            }
        }'
    );"#
    .execute_result(&mut conn);

    assert!(result.is_err());
}

#[rstest]
fn alias_chain_validation(mut conn: PgConnection) {
    // Test that we can't create an alias that points to another alias
    "CREATE TABLE alias_chain (
        id SERIAL PRIMARY KEY,
        base_field TEXT
    );"
    .execute(&mut conn);

    let result = r#"CREATE INDEX alias_chain_idx ON alias_chain
    USING bm25 (id, base_field)
    WITH (
        key_field='id',
        text_fields='{
            "first_alias": {
                "column": "base_field",
                "tokenizer": {"type": "default"}
            },
            "second_alias": {
                "column": "first_alias",
                "tokenizer": {"type": "default"}
            }
        }'
    );"#
    .execute_result(&mut conn);

    assert!(result.is_err());
}

#[rstest]
fn mixed_field_types_with_aliases(mut conn: PgConnection) {
    // Test mixing different field types with aliases
    "CREATE TABLE mixed_fields (
        id SERIAL PRIMARY KEY,
        text_content TEXT,
        num_value INTEGER,
        bool_flag BOOLEAN
    );"
    .execute(&mut conn);

    "INSERT INTO mixed_fields (text_content, num_value, bool_flag) VALUES 
    ('test content', 42, true),
    ('another test', 100, false);"
        .execute(&mut conn);

    r#"CREATE INDEX mixed_fields_idx ON mixed_fields
    USING bm25 (id, text_content, num_value, bool_flag)
    WITH (
        key_field='id',
        text_fields='{
            "text_alias": {
                "column": "text_content",
                "tokenizer": {"type": "default"}
            }
        }',
        numeric_fields='{
            "num_alias": {
                "column": "num_value"
            }
        }',
        boolean_fields='{
            "bool_alias": {
                "column": "bool_flag"
            }
        }'
    );"#
    .execute(&mut conn);

    // Test each type of alias
    let rows: Vec<(i32,)> =
        "SELECT id FROM mixed_fields WHERE mixed_fields @@@ 'text_alias:test'".fetch(&mut conn);
    assert_eq!(rows.len(), 2);

    let rows: Vec<(i32,)> =
        "SELECT id FROM mixed_fields WHERE mixed_fields @@@ 'num_alias:42'".fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(i32,)> =
        "SELECT id FROM mixed_fields WHERE mixed_fields @@@ 'bool_alias:true'".fetch(&mut conn);
    assert_eq!(rows.len(), 1);
}
