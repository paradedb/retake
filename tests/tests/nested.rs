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
use fixtures::*;
use pretty_assertions::assert_eq;
use rstest::*;
use sqlx::PgConnection;

#[rstest]
async fn nested_single_level_simple(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Create a table with a single level of nested JSON data
    r#"
    CREATE TABLE nested_table_single (
        id SERIAL PRIMARY KEY,
        nested_data JSONB
    );
    "#
    .execute(&mut conn);

    // std::thread::sleep(std::time::Duration::from_secs(10));

    // Insert some documents
    r#"
    INSERT INTO nested_table_single (nested_data) VALUES
    ('{"obj1": {"name": "blue", "count": 10}}'),
    ('{"obj1": {"name": "red",  "count": 4}}'),
    ('{"obj1": {"name": "blue", "count": 3}}');
    "#
    .execute(&mut conn);

    // Create a bm25 index that marks nested_data as a nested field
    r#"
    CREATE INDEX nested_table_single_idx ON nested_table_single
    USING bm25 (id, nested_data)
    WITH (
        key_field = 'id',
        json_fields = '{"nested_data": {"nested": {"obj1": {}}}}'
    );
    "#
    .execute(&mut conn);

    // Query for documents where nested_data.obj1.name = "blue" and nested_data.obj1.count > 5
    let rows: Vec<(i32,)> = r#"
        SELECT id FROM nested_table_single
        WHERE nested_table_single @@@ paradedb.nested(
            path => 'nested_data',
            query => paradedb.boolean(
                must => ARRAY[
                    paradedb.term(field => 'nested_data.obj1.name', value => 'blue'),
                    paradedb.term(field => 'nested_data.obj1.count', value => 10)
                ]
            )
        )
        ORDER BY id
        "#
    .fetch(&mut conn);

    // Only the first row matches both conditions
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1,));

    Ok(())
}

#[rstest]
async fn nested_single_level_score_modes(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Create a table with a single level of nested JSON data
    r#"
    CREATE TABLE nested_table_score (
        id SERIAL PRIMARY KEY,
        nested_data JSONB
    );
    "#
    .execute(&mut conn);

    // Insert data
    r#"
    INSERT INTO nested_table_score (nested_data) VALUES
    ('{"obj1": [{"name": "powell", "value": 3}, {"name": "powell", "value": 7}]}'),
    ('{"obj1": [{"name": "powell", "value": 10}, {"name": "powell", "value": 2}]}');
    "#
    .execute(&mut conn);

    std::thread::sleep(std::time::Duration::from_secs(10));

    // Create index
    r#"
    CREATE INDEX nested_table_score_idx ON nested_table_score
    USING bm25 (id, nested_data)
    WITH (
        key_field = 'id',
        json_fields = '{
            "nested_data": {
                "nested": {
                    "obj1": {}
                }
            }
        }'
    );
    "#
    .execute(&mut conn);

    let rows_avg: Vec<(i32,)> = r#"
        SELECT id FROM nested_table_score
        WHERE nested_table_score @@@ paradedb.nested(
            path => 'nested_data',
            query => paradedb.nested(
                path => 'nested_data.obj1',
                query => paradedb.term(field => 'nested_data.obj1.name', value => 'powell')
            )
        )
        ORDER BY id
        "#
    .fetch(&mut conn);

    // Both documents should match
    assert_eq!(rows_avg.len(), 2);
    assert_eq!(rows_avg[0], (1,));
    assert_eq!(rows_avg[1], (2,));

    Ok(())
}

#[rstest]
async fn nested_multi_level(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Create a table with multi-level nested JSON data
    r#"
    CREATE TABLE nested_table_multi (
        id SERIAL PRIMARY KEY,
        nested_data JSONB
    );
    "#
    .execute(&mut conn);

    // Insert data with deeper nesting
    r#"
    INSERT INTO nested_table_multi (nested_data) VALUES
    (
        '{
            "driver": {
                "last_name": "McQueen",
                "vehicle": [
                    {"make": "Powell Motors", "model": "Canyonero"},
                    {"make": "Miller-Meteor", "model": "Ecto-1"}
                ]
            }
        }'
    ),
    (
        '{
            "driver": {
                "last_name": "Hudson",
                "vehicle": [
                    {"make": "Mifune", "model": "Mach Five"},
                    {"make": "Miller-Meteor", "model": "Ecto-1"}
                ]
            }
        }'
    );
    "#
    .execute(&mut conn);

    // Create index with nested fields
    r#"
    CREATE INDEX nested_table_multi_idx ON nested_table_multi
    USING bm25 (id, nested_data)
    WITH (
        key_field = 'id',
        json_fields = '{
            "nested_data": {
                "nested": {
                    "driver": {
                        "nested": {
                            "vehicle": {}
                        }
                    }
                }
            }
        }'
    );
    "#
    .execute(&mut conn);

    // Query for multi-level nested data: driver.vehicle.make=Powell Motors AND driver.vehicle.model=Canyonero
    let rows: Vec<(i32,)> =  r#"
        SELECT id FROM nested_table_multi
        WHERE nested_table_multi @@@ paradedb.nested(
            path => 'nested_data',
            query => paradedb.nested(
                path => 'nested_data.driver',
                query => paradedb.nested(
                    path => 'nested_data.driver.vehicle',
                    query => paradedb.boolean(
                        must => ARRAY[
                            paradedb.term(field => 'nested_data.driver.vehicle.make', value => 'powell'),
                            paradedb.term(field => 'nested_data.driver.vehicle.model', value => 'canyonero')
                        ]
                    )
                )
            )
        )
        ORDER BY id
        "#.fetch(&mut conn);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1,));

    Ok(())
}

#[rstest]
async fn nested_must_not_clause(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Create a table to test must_not clauses within nested data
    r#"
    CREATE TABLE nested_table_must_not (
        id SERIAL PRIMARY KEY,
        posts JSONB
    );
    "#
    .execute(&mut conn);

    // Insert sample docs
    r#"
    INSERT INTO nested_table_must_not (posts) VALUES
    ('{"comments": [{"author": "kimchy"}]}'),
    ('{"comments": [{"author": "kimchy"}, {"author": "nik9000"}]}'),
    ('{"comments": [{"author": "nik9000"}]}');
    "#
    .execute(&mut conn);

    // Create index
    r#"
    CREATE INDEX nested_table_must_not_idx ON nested_table_must_not
    USING bm25 (id, posts)
    WITH (
        key_field = 'id',
        json_fields = '{
            "posts": {
                "tokenizer": {"type": "raw"},
                "nested": {
                    "comments": {}
                }
            }
        }'
    );
    "#
    .execute(&mut conn);

    // This query returns docs where the nested subdocument does NOT have "author=nik9000"
    // but it will still return any doc that has at least one nested subdocument matching the must_not.
    let rows_inner: Vec<(i32,)> = r#"
        SELECT id FROM nested_table_must_not
        WHERE nested_table_must_not @@@ paradedb.nested(
            path => 'posts',
            query => paradedb.nested(
                path => 'posts.comments',
                query => paradedb.boolean(
                    must => ARRAY[
                        paradedb.all()  
                    ],
                    must_not => ARRAY[
                        paradedb.term(field => 'posts.comments.author', value => 'nik9000')
                    ]
                )
            )
        )
        ORDER BY id
        "#
    .fetch(&mut conn);

    // Document 1 and 2 both match the inner must_not for at least one subdocument
    assert_eq!(rows_inner.len(), 2);
    assert_eq!(rows_inner[0], (1,));
    assert_eq!(rows_inner[1], (2,));

    // Now place the must_not at the outer level to exclude any doc that has ANY subdoc with author=nik9000
    let rows_outer: Vec<(i32,)> = r#"
        SELECT id FROM nested_table_must_not
        WHERE nested_table_must_not @@@ paradedb.boolean(
            must => ARRAY[
                paradedb.all()
            ],
            must_not => ARRAY[
                paradedb.nested(
                    path => 'posts',
                    query => paradedb.nested(
                        path => 'posts.comments',
                        query => paradedb.term(field => 'posts.comments.author', value => 'nik9000')
                    )
                )
            ]
        )
        ORDER BY id
        "#
    .fetch(&mut conn);

    println!("ROWS_OUTER {:?}", rows_outer);
    // Only doc 1 is returned, because doc 2 and doc 3 contain 'nik9000' in at least one subdocument
    assert_eq!(rows_outer.len(), 1);
    assert_eq!(rows_outer[0], (1,));

    Ok(())
}

#[rstest]
async fn nested_ignore_unmapped_path(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Create a table with some nested data
    r#"
    CREATE TABLE nested_table_unmapped (
        id SERIAL PRIMARY KEY,
        data JSONB
    );
    "#
    .execute(&mut conn);

    // Insert data
    r#"
    INSERT INTO nested_table_unmapped (data) VALUES
    ('{"obj1": [{"name": "apple"}]}'),
    ('{"obj1": [{"name": "banana"}]}');
    "#
    .execute(&mut conn);

    // Create index for the existing path
    r#"
    CREATE INDEX nested_table_unmapped_idx ON nested_table_unmapped
    USING bm25 (id, data)
    WITH (
        key_field = 'id',
        json_fields = '{
            "data": {
                "nested": {
                    "obj1": {}
                }
            }
        }'
    );
    "#
    .execute(&mut conn);

    // Query with ignore_unmapped => true, referencing an unmapped path "data.obj2"
    let rows_ignore_unmapped: Vec<(i32,)> = r#"
        SELECT id FROM nested_table_unmapped
        WHERE nested_table_unmapped @@@ paradedb.nested(
            path => 'data',
            query => paradedb.nested(
                path => 'data.obj2',
                query => paradedb.term(field => 'data.obj2.whatever', value => 'none'),
                ignore_unmapped => true
            )
        )
        ORDER BY id
        "#
    .fetch(&mut conn);

    // Should return nothing, but not error out
    assert_eq!(rows_ignore_unmapped.len(), 0);

    // Query with ignore_unmapped => false should produce an error
    // We'll confirm we get an error containing 'unmapped path' or similar
    let err = r#"
        SELECT id FROM nested_table_unmapped
        WHERE nested_table_unmapped @@@ paradedb.nested(
            path => 'data',
            query => paradedb.nested(
                path => 'data.obj2',
                query => paradedb.term(field => 'data.obj2.whatever', value => 'none'),
                ignore_unmapped => false
            )
        )
        ORDER BY id
        "#
    .fetch_result::<(i32,)>(&mut conn);

    println!("ERR {err:?}");
    assert_eq!(
        err.err().map(|e| e.to_string()),
        Some("error returned from database: weight should be constructable: SchemaError(\"NestedQuery path 'data.obj2' not mapped, and ignore_unmapped=false\")".into())
    );

    Ok(())
}

#[rstest]
async fn nested_cart_products_score_modes(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Same table & data structure, but let's add more data to test different score modes
    r#"
    CREATE TABLE nested_shopping_carts_scores (
        id SERIAL PRIMARY KEY,
        cart JSONB
    );
    "#
    .execute(&mut conn);

    // Insert data with multiple products having partial matches, so we can test whether
    // the different scoring modes produce different match sets or the same set with different scores.
    r#"
    INSERT INTO nested_shopping_carts_scores (cart) VALUES
    (
        '{
            "cart_id": 400,
            "products": [
                {
                    "product_id": 40,
                    "model_attributes": {
                        "color": "blue",
                        "size": "L"
                    }
                },
                {
                    "product_id": 41,
                    "model_attributes": {
                        "color": "blue",
                        "size": "M"
                    }
                }
            ]
        }'
    ),
    (
        '{
            "cart_id": 500,
            "products": [
                {
                    "product_id": 42,
                    "model_attributes": {
                        "color": "blue",
                        "size": "M"
                    }
                },
                {
                    "product_id": 44,
                    "model_attributes": {
                        "color": "red",
                        "size": "L"
                    }
                }
            ]
        }'
    ),
    (
        '{
            "cart_id": 600,
            "products": [
                {
                    "product_id": 48,
                    "model_attributes": {
                        "color": "blue",
                        "size": "L"
                    }
                },
                {
                    "product_id": 49,
                    "model_attributes": {
                        "color": "blue",
                        "size": "L"
                    }
                }
            ]
        }'
    );
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX nested_shopping_carts_scores_idx ON nested_shopping_carts_scores
    USING bm25 (id, cart)
    WITH (
        key_field = 'id',
        json_fields = '{
            "cart": {
                "nested": {
                    "products": {}
                }
            }
        }'
    );
    "#
    .execute(&mut conn);

    // We'll run the same logical query, but with different score_mode settings.
    // For demonstration, only 'avg' and 'sum' or 'none' might behave differently,
    // but let's test them just to confirm the index doesn't blow up.
    // We'll ensure that all three docs match because they have at least one product with color=blue,
    // but the final scoring might differ. (In real usage we'd check actual scores, but
    // here we're just testing that the search doesn't fail and the correct docs are returned.)

    let rows_avg: Vec<(i32,)> = r#"
        SELECT id FROM nested_shopping_carts_scores
        WHERE nested_shopping_carts_scores @@@ paradedb.nested(
            path => 'cart',
            query => paradedb.nested(
                path => 'cart.products',
                query => paradedb.term(field => 'cart.products.model_attributes.color', value => 'blue')
            )
        )
        ORDER BY id
        "#.fetch(&mut conn);
    // Expect all (1,2,3) in ascending order by ID in this new table, which is 1.. for the table rows
    assert_eq!(rows_avg.len(), 3);

    let rows_sum: Vec<(i32,)> = r#"
        SELECT id FROM nested_shopping_carts_scores
        WHERE nested_shopping_carts_scores @@@ paradedb.nested(
            path => 'cart',
            query => paradedb.nested(
                path => 'cart.products',
                query => paradedb.term(field => 'cart.products.model_attributes.color', value => 'blue'),
                score_mode => 'Total'
            ),
            score_mode => 'Total'
        )
        ORDER BY id
        "#.fetch(&mut conn);
    assert_eq!(rows_sum.len(), 3);

    let rows_none: Vec<(i32,)> = r#"
        SELECT id FROM nested_shopping_carts_scores
        WHERE nested_shopping_carts_scores @@@ paradedb.nested(
            path => 'cart.products',
            query => paradedb.nested(
                path => 'cart.products',
                query => paradedb.term(field => 'cart.products.model_attributes.color', value => 'blue'),
                score_mode => 'None'
            ),
            score_mode => 'None'
        )
        ORDER BY id
        "#.fetch(&mut conn);
    assert_eq!(rows_none.len(), 0);

    Ok(())
}

#[rstest]
async fn nested_cart_products_partial_mismatch(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Create a table for partial mismatch checks, where some product lines
    // have only color, others have only size, etc. We want to confirm that
    // the entire cart is only returned if the single nested product matches
    // the entire search condition.

    r#"
    CREATE TABLE nested_carts_partial_mismatch (
        id SERIAL PRIMARY KEY,
        cart JSONB
    );
    "#
    .execute(&mut conn);

    r#"
    INSERT INTO nested_carts_partial_mismatch (cart) VALUES
    (
        '{
            "cart_id": 700,
            "products": [
                {
                    "product_id": 70,
                    "model_attributes": {
                        "color": "blue"
                    }
                },
                {
                    "product_id": 71,
                    "model_attributes": {
                        "size": "L"
                    }
                }
            ]
        }'
    ),
    (
        '{
            "cart_id": 800,
            "products": [
                {
                    "product_id": 72,
                    "model_attributes": {
                        "color": "blue",
                        "size": "L"
                    }
                }
            ]
        }'
    ),
    (
        '{
            "cart_id": 900,
            "products": [
                {
                    "product_id": 73,
                    "model_attributes": {
                        "color": "red",
                        "size": "L"
                    }
                }
            ]
        }'
    );
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX nested_carts_partial_mismatch_idx ON nested_carts_partial_mismatch
    USING bm25 (id, cart)
    WITH (
        key_field = 'id',
        json_fields = '{
            "cart": {
                "nested": {
                    "products": {}
                }
            }
        }'
    );
    "#
    .execute(&mut conn);

    // Now let's query for color=blue AND size=L in the same product.
    // Only cart_id=800 truly has a single product with both color=blue and size=L.
    // Cart_id=700 has color=blue in product_id=70, size=L in product_id=71, but
    // that's not the same product.
    // Cart_id=900 is color=red size=L, so mismatch as well.

    let rows: Vec<(i32,)> = r#"
        SELECT id FROM nested_carts_partial_mismatch
        WHERE nested_carts_partial_mismatch @@@ paradedb.nested(
            path => 'cart',
            query => paradedb.nested(
                path => 'cart.products',
                query => paradedb.boolean(
                    must => ARRAY[
                        paradedb.term(field => 'cart.products.model_attributes.color', value => 'blue'),
                        paradedb.term(field => 'cart.products.model_attributes.size', value => 'l')
                    ]
                )
            )
        )
        ORDER BY id
        "#.fetch(&mut conn);

    // Only row #2 matches.
    assert_eq!(rows, vec![(2,)]);

    Ok(())
}

#[rstest]
async fn nested_cart_products_mix_and_match(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Test scenario where a cart has multiple nested products,
    // some of which match color=blue & size=L, and others don't match at all.
    // This confirms we only need at least one matching product for the parent cart to appear.

    r#"
    CREATE TABLE nested_carts_mix_and_match (
        id SERIAL PRIMARY KEY,
        cart JSONB
    );
    "#
    .execute(&mut conn);

    r#"
    INSERT INTO nested_carts_mix_and_match (cart) VALUES
    (
        '{
            "cart_id": 1000,
            "products": [
                {
                    "product_id": 100,
                    "model_attributes": {
                        "color": "red",
                        "size": "L"
                    }
                },
                {
                    "product_id": 101,
                    "model_attributes": {
                        "color": "blue",
                        "size": "L"
                    }
                },
                {
                    "product_id": 102,
                    "model_attributes": {
                        "color": "green",
                        "size": "S"
                    }
                }
            ]
        }'
    ),
    (
        '{
            "cart_id": 1100,
            "products": [
                {
                    "product_id": 110,
                    "model_attributes": {
                        "color": "red",
                        "size": "L"
                    }
                },
                {
                    "product_id": 111,
                    "model_attributes": {
                        "color": "red",
                        "size": "M"
                    }
                },
                {
                    "product_id": 112,
                    "model_attributes": {
                        "color": "blue",
                        "size": "S"
                    }
                }
            ]
        }'
    );
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX nested_carts_mix_and_match_idx ON nested_carts_mix_and_match
    USING bm25 (id, cart)
    WITH (
        key_field = 'id',
        json_fields = '{
            "cart": {
                "nested": {
                    "products": {}
                }
            }
        }'
    );
    "#
    .execute(&mut conn);

    // Now let's run a nested query for color=blue AND size=L.
    // The first cart (id=1) has a product that meets that criterion (product_id=101).
    // The second cart (id=2) doesn't, so it shouldn't show up.

    let rows: Vec<(i32,)> = r#"
        SELECT id FROM nested_carts_mix_and_match
        WHERE nested_carts_mix_and_match @@@ paradedb.nested(
            path => 'cart',
            query => paradedb.nested(
                path => 'cart.products',
                query => paradedb.boolean(
                    must => ARRAY[
                        paradedb.term(field => 'cart.products.model_attributes.color', value => 'blue'),
                        paradedb.term(field => 'cart.products.model_attributes.size', value => 'l')
                    ]
                )
            )
        )
        ORDER BY id
        "#.fetch(&mut conn);

    // Only row #1 matches
    assert_eq!(rows, vec![(1,)]);

    Ok(())
}
