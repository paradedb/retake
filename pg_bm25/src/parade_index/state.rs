use crate::index_access::utils::SearchConfig;
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, RegexQuery};
use tantivy::query_grammar::Occur;
use tantivy::{
    query::{Query, QueryParser},
    schema::*,
    DocAddress, Score, Searcher,
};

use super::index::ParadeIndex;

pub struct TantivyScanState {
    pub schema: Schema,
    pub query: Box<dyn Query>,
    pub parser: QueryParser,
    pub searcher: Searcher,
    pub iterator: *mut std::vec::IntoIter<(Score, DocAddress)>,
    pub config: SearchConfig,
    pub key_field_name: String,
}

impl TantivyScanState {
    pub fn new(parade_index: &ParadeIndex, config: &SearchConfig) -> Self {
        let schema = parade_index.schema();
        let mut parser = parade_index.query_parser();
        let query = Self::query(config, &schema, &mut parser);
        TantivyScanState {
            schema,
            query,
            parser,
            config: config.clone(),
            searcher: parade_index.searcher(),
            iterator: std::ptr::null_mut(),
            key_field_name: parade_index.key_field_name.clone(),
        }
    }

    pub fn key_field_value(&mut self, doc_address: DocAddress) -> i64 {
        let retrieved_doc = self.searcher.doc(doc_address).expect("could not find doc");

        let key_field = self
            .schema
            .get_field(&self.key_field_name)
            .expect("field '{key_field_name}' not found in schema");

        if let tantivy::schema::Value::I64(key_field_value) =
            retrieved_doc.get_first(key_field).unwrap_or_else(|| {
                panic!(
                    "value for key_field '{}' not found in doc",
                    &self.key_field_name,
                )
            })
        {
            *key_field_value
        } else {
            panic!("error unwrapping ctid value")
        }
    }

    pub fn search(&mut self) -> Vec<(f32, DocAddress)> {
        // Extract limit and offset from the query config or set defaults.
        let limit = self.config.limit_rows.unwrap_or_else(|| {
            // We use unwrap_or_else here so this block doesn't run unless
            // we actually need the default value. This is important, because there can
            // be some cost to Tantivy API calls.
            let num_docs = self.searcher.num_docs() as usize;
            if num_docs > 0 {
                num_docs // The collector will panic if it's passed a limit of 0.
            } else {
                1 // Since there's no docs to return anyways, just use 1.
            }
        });

        let offset = self.config.offset_rows.unwrap_or(0);

        self.searcher
            .search(&self.query, &TopDocs::with_limit(limit).and_offset(offset))
            .expect("failed to search")
    }

    pub fn doc(&self, doc_address: DocAddress) -> tantivy::Result<Document> {
        self.searcher.doc(doc_address)
    }

    fn query(
        query_config: &SearchConfig,
        schema: &Schema,
        parser: &mut QueryParser,
    ) -> Box<dyn Query> {
        let fuzzy_fields = &query_config.fuzzy_fields;
        let regex_fields = &query_config.regex_fields;

        // Determine if we're using regex fields based on the presence or absence of prefix and fuzzy fields.
        // It panics if both are provided as that's considered an invalid input.
        let using_regex_fields = match (!regex_fields.is_empty(), !fuzzy_fields.is_empty()) {
            (true, true) => panic!("cannot search with both regex_fields and fuzzy_fields"),
            (true, false) => true,
            _ => false,
        };

        // Construct the actual Tantivy search query based on the mode determined above.
        let tantivy_query: Box<dyn Query> = if using_regex_fields {
            let regex_pattern = format!("{}.*", &query_config.query);
            let mut queries: Vec<Box<dyn Query>> = Vec::new();

            // Build a regex query for each specified regex field.
            for field_name in &mut regex_fields.iter() {
                if let Ok(field) = schema.get_field(field_name) {
                    let regex_query =
                        Box::new(RegexQuery::from_pattern(&regex_pattern, field).unwrap());
                    queries.push(regex_query);
                }
            }

            // If there's only one query, use it directly; otherwise, combine the queries.
            if queries.len() == 1 {
                queries.remove(0)
            } else {
                let boolean_query =
                    BooleanQuery::new(queries.into_iter().map(|q| (Occur::Should, q)).collect());
                Box::new(boolean_query)
            }
        } else {
            let require_prefix = query_config.prefix.unwrap_or(true);
            let transpose_cost_one = query_config.transpose_cost_one.unwrap_or(true);
            let max_distance = query_config.distance.unwrap_or(2);

            for field_name in &mut fuzzy_fields.iter() {
                if let Ok(field) = schema.get_field(field_name) {
                    parser.set_field_fuzzy(field, require_prefix, max_distance, transpose_cost_one);
                }
            }

            // Construct the query using the lenient parser to tolerate minor errors in the input.
            parser.parse_query_lenient(&query_config.query).0
        };

        tantivy_query
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::*;
    use shared::testing::{test_table, ExpectedRow, SETUP_SQL};

    #[pg_test]
    fn test_basic_search_query() -> spi::Result<()> {
        Spi::run(SETUP_SQL).expect("failed to setup index");
        Spi::connect(|client| {
            let table = client.select(
                "SELECT * FROM bm25_search.search('description:keyboard OR category:electronics');",
                None,
                None,
            )?;

            let expect = vec![
                ExpectedRow {
                    id: Some(2),
                    description: Some("Plastic Keyboard"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    in_stock: Some(false),
                    metadata: Some(serde_json::json!({"color": "Black", "location": "Canada"})),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(1),
                    description: Some("Ergonomic metal keyboard"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    in_stock: Some(true),
                    metadata: Some(
                        serde_json::json!({"color": "Silver", "location": "United States"}),
                    ),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(12),
                    description: Some("Innovative wireless earbuds"),
                    rating: Some(5),
                    category: Some("Electronics"),
                    in_stock: Some(true),
                    metadata: Some(serde_json::json!({"color": "Black", "location": "China"})),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(22),
                    description: Some("Fast charging power bank"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    in_stock: Some(true),
                    metadata: Some(
                        serde_json::json!({"color": "Black", "location": "United States"}),
                    ),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(32),
                    description: Some("Bluetooth-enabled speaker"),
                    rating: Some(3),
                    category: Some("Electronics"),
                    in_stock: Some(true),
                    metadata: Some(serde_json::json!({"color": "Black", "location": "Canada"})),
                    ..Default::default() // Other fields default to None
                },
            ];

            let _ = test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    fn test_basic_search_with_limit_query() -> spi::Result<()> {
        Spi::run(SETUP_SQL).expect("failed to setup index");
        Spi::connect(|client| {
            let table = client.select(
                "SELECT id, description, rating, category FROM search_config.search('category:electronics', limit_rows => 2);",
                None,
                None,
            )?;

            let expect = vec![
                ExpectedRow {
                    id: Some(1),
                    description: Some("Ergonomic metal keyboard"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(2),
                    description: Some("Plastic Keyboard"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    ..Default::default() // Other fields default to None
                },
            ];

            let _ = test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    fn test_basic_search_with_fuzzy_field_query() -> spi::Result<()> {
        Spi::run(SETUP_SQL).expect("failed to setup index");
        Spi::connect(|client| {
            let table = client.select(
                "SELECT id, description, rating, category FROM search_config.search('category:electornics', fuzzy_fields => 'category');",
                None,
                None,
            )?;

            let expect = vec![
                ExpectedRow {
                    id: Some(1),
                    description: Some("Ergonomic metal keyboard"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(2),
                    description: Some("Plastic Keyboard"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(12),
                    description: Some("Innovative wireless earbuds"),
                    rating: Some(5),
                    category: Some("Electronics"),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(22),
                    description: Some("Fast charging power bank"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(32),
                    description: Some("Bluetooth-enabled speaker"),
                    rating: Some(3),
                    category: Some("Electronics"),
                    ..Default::default() // Other fields default to None
                },
            ];

            let _ = test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    fn test_basic_search_without_fuzzy_field_query() -> spi::Result<()> {
        Spi::run(SETUP_SQL).expect("failed to setup index");
        Spi::connect(|client| {
            let table = client.select(
                "SELECT id, description, rating, category FROM search_config.search('category:electornics');",
                None,
                None,
            )?;

            // Test search with without fuzzy field and with typo: no results
            let expect = vec![];

            let _ = test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    fn search_fuzzy_field_transpose_cost_false_distance_one_query() -> spi::Result<()> {
        Spi::run(SETUP_SQL).expect("failed to setup index");
        Spi::connect(|client| {
            let table = client.select(
                "SELECT id, description, rating, category FROM search_config.search('description:keybaord', fuzzy_fields => 'description', transpose_cost_one => false, distance => 1);",
                None,
                None,
            )?;

            // Test search with without fuzzy field and with typo: no results
            let expect = vec![];

            let _ = test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    fn search_fuzzy_field_transpose_cost_true_distance_one_query() -> spi::Result<()> {
        Spi::run(SETUP_SQL).expect("failed to setup index");
        Spi::connect(|client| {
            let table = client.select(
                "SELECT id, description, rating, category FROM search_config.search('description:keybaord', fuzzy_fields => 'description', transpose_cost_one => true, distance => 1);",
                None,
                None,
            )?;

            let expect = vec![
                ExpectedRow {
                    id: Some(1),
                    description: Some("Ergonomic metal keyboard"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(2),
                    description: Some("Plastic Keyboard"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    ..Default::default() // Other fields default to None
                },
            ];

            let _ = test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    fn search_regex_field() -> spi::Result<()> {
        Spi::run(SETUP_SQL).expect("failed to setup index");
        Spi::connect(|client| {
            let table = client.select(
                "SELECT id, description, rating, category FROM search_config.search('com', regex_fields => 'description');",
                None,
                None,
            )?;

            let expect = vec![
                ExpectedRow {
                    id: Some(6),
                    description: Some("Compact digital camera"),
                    rating: Some(5),
                    category: Some("Photography"),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(23),
                    description: Some("Comfortable slippers"),
                    rating: Some(3),
                    category: Some("Footwear"),
                    ..Default::default() // Other fields default to None
                },
            ];

            let _ = test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    fn test_json_search_query() -> spi::Result<()> {
        Spi::run(SETUP_SQL).expect("failed to setup index");
        Spi::connect(|client| {
            let table = client.select(
                "SELECT * FROM bm25_search.search('metadata.color:white');",
                None,
                None,
            )?;

            let expect = vec![
                ExpectedRow {
                    id: Some(4),
                    description: Some("White jogging shoes"),
                    rating: Some(3),
                    category: Some("Footwear"),
                    in_stock: Some(false),
                    metadata: Some(
                        serde_json::json!({"color": "White", "location": "United States"}),
                    ),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(15),
                    description: Some("Refreshing face wash"),
                    rating: Some(2),
                    category: Some("Beauty"),
                    in_stock: Some(false),
                    metadata: Some(serde_json::json!({"color": "White", "location": "China"})),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(25),
                    description: Some("Anti-aging serum"),
                    rating: Some(4),
                    category: Some("Beauty"),
                    in_stock: Some(true),
                    metadata: Some(
                        serde_json::json!({"color": "White", "location": "United States"}),
                    ),
                    ..Default::default() // Other fields default to None
                },
            ];

            let _ = test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    fn test_default_tokenizer_no_results_search_query() -> spi::Result<()> {
        Spi::run(SETUP_SQL).expect("failed to setup index");
        Spi::connect(|client| {
            let table = client.select(
                "SELECT * FROM bm25_search.search('description:earbud');",
                None,
                None,
            )?;

            // Test search with default tokenizer: no results
            let expect = vec![];

            let _ = test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    fn test_search_with_bm25_scoring_query() -> spi::Result<()> {
        Spi::run(SETUP_SQL).expect("failed to setup index");
        Spi::connect(|client| {
            let table = client.select(
                "SELECT r.rank_bm25, s.* FROM bm25_search.search('category:electronics OR description:keyboard') as s LEFT JOIN bm25_search.rank('category:electronics OR description:keyboard') as r ON s.id = r.id;",
                None,
                None,
            )?;

            let expect = vec![
                ExpectedRow {
                    rank_bm25: Some(4.931014),
                    id: Some(1),
                    description: Some("Ergonomic metal keyboard"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    in_stock: Some(true),
                    metadata: Some(
                        serde_json::json!({"color": "Silver", "location": "United States"}),
                    ),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    rank_bm25: Some(5.3764954),
                    id: Some(2),
                    description: Some("Plastic Keyboard"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    in_stock: Some(false),
                    metadata: Some(serde_json::json!({"color": "Black", "location": "Canada"})),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    rank_bm25: Some(2.1096356),
                    id: Some(12),
                    description: Some("Innovative wireless earbuds"),
                    rating: Some(5),
                    category: Some("Electronics"),
                    in_stock: Some(true),
                    metadata: Some(serde_json::json!({"color": "Black", "location": "China"})),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    rank_bm25: Some(2.1096356),
                    id: Some(22),
                    description: Some("Fast charging power bank"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    in_stock: Some(true),
                    metadata: Some(
                        serde_json::json!({"color": "Black", "location": "United States"}),
                    ),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    rank_bm25: Some(2.1096356),
                    id: Some(32),
                    description: Some("Bluetooth-enabled speaker"),
                    rating: Some(3),
                    category: Some("Electronics"),
                    in_stock: Some(true),
                    metadata: Some(serde_json::json!({"color": "Black", "location": "Canada"})),
                    ..Default::default() // Other fields default to None
                },
            ];

            let _ = test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    fn test_quoted_table_name_search() {
        Spi::run(SETUP_SQL).expect("failed to setup index");

        // Execute the query and retrieve the result
        let (key, name, age) =
            Spi::get_three::<i32, String, i32>("SELECT * FROM activity.search('name:alice')")
                .expect("failed to query");

        // Assert that the retrieved values match the expected output
        assert_eq!(key, Some(1));
        assert_eq!(name, Some("Alice".to_string()));
        assert_eq!(age, Some(29));
    }

    #[pg_test]
    fn test_real_time_search() -> spi::Result<()> {
        Spi::run(SETUP_SQL).expect("failed to setup index");
        Spi::run("SET search_path TO paradedb;").expect("failed to set search path");
        Spi::run("INSERT INTO bm25_search (description, rating, category) VALUES ('New keyboard', 5, 'Electronics'); DELETE FROM bm25_search WHERE id = 1; UPDATE bm25_search SET description = 'PVC Keyboard' WHERE id = 2;").expect("failed to setup index");
        Spi::connect(|client| {
            let table = client.select(
                "SELECT * FROM bm25_search.search('description:keyboard OR category:electronics');",
                None,
                None,
            )?;

            let expect = vec![
                ExpectedRow {
                    id: Some(42),
                    description: Some("New keyboard"),
                    rating: Some(5),
                    category: Some("Electronics"),
                    in_stock: None,
                    metadata: None,
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(2),
                    description: Some("PVC Keyboard"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    in_stock: Some(false),
                    metadata: Some(serde_json::json!({"color": "Black", "location": "Canada"})),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(12),
                    description: Some("Innovative wireless earbuds"),
                    rating: Some(5),
                    category: Some("Electronics"),
                    in_stock: Some(true),
                    metadata: Some(serde_json::json!({"color": "Black", "location": "China"})),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(22),
                    description: Some("Fast charging power bank"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    in_stock: Some(true),
                    metadata: Some(
                        serde_json::json!({"color": "Black", "location": "United States"}),
                    ),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(32),
                    description: Some("Bluetooth-enabled speaker"),
                    rating: Some(3),
                    category: Some("Electronics"),
                    in_stock: Some(true),
                    metadata: Some(serde_json::json!({"color": "Black", "location": "Canada"})),
                    ..Default::default() // Other fields default to None
                },
            ];

            let _ = test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    fn test_sequential_scan_syntax() -> spi::Result<()> {
        Spi::run(SETUP_SQL).expect("failed to setup index");
        Spi::connect(|client| {
            let table = client.select(
                "SELECT * FROM paradedb.bm25_test_table WHERE paradedb.search_tantivy(paradedb.bm25_test_table.*,jsonb_build_object('index_name', 'bm25_search_bm25_index','table_name', 'bm25_test_table','schema_name', 'paradedb','key_field', 'id','query', 'category:electronics'));",
                None,
                None,
            )?;

            let expect = vec![
                ExpectedRow {
                    id: Some(1),
                    description: Some("Ergonomic metal keyboard"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    in_stock: Some(true),
                    metadata: Some(
                        serde_json::json!({"color": "Silver", "location": "United States"}),
                    ),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(2),
                    description: Some("Plastic Keyboard"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    in_stock: Some(false),
                    metadata: Some(serde_json::json!({"color": "Black", "location": "Canada"})),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(12),
                    description: Some("Innovative wireless earbuds"),
                    rating: Some(5),
                    category: Some("Electronics"),
                    in_stock: Some(true),
                    metadata: Some(serde_json::json!({"color": "Black", "location": "China"})),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(22),
                    description: Some("Fast charging power bank"),
                    rating: Some(4),
                    category: Some("Electronics"),
                    in_stock: Some(true),
                    metadata: Some(
                        serde_json::json!({"color": "Black", "location": "United States"}),
                    ),
                    ..Default::default() // Other fields default to None
                },
                ExpectedRow {
                    id: Some(32),
                    description: Some("Bluetooth-enabled speaker"),
                    rating: Some(3),
                    category: Some("Electronics"),
                    in_stock: Some(true),
                    metadata: Some(serde_json::json!({"color": "Black", "location": "Canada"})),
                    ..Default::default() // Other fields default to None
                },
            ];

            let _ = test_table(table, expect);

            Ok(())
        })
    }

    #[pg_test]
    fn test_text_arrays() {
        Spi::run(SETUP_SQL).expect("failed to setup index");

        let (id, text_array, varchar_array) = Spi::get_three("SELECT id, text_array::TEXT, varchar_array::TEXT FROM example_table.search('text_array:text1');")
            .expect("failed to query");
        assert_eq!(id, Some(1));
        assert_eq!(text_array, Some("{text1,text2,text3}".to_string()));
        assert_eq!(varchar_array, Some("{vtext1,vtext2}".to_string()));

        let (id, text_array, varchar_array) = Spi::get_three("SELECT id, text_array::TEXT, varchar_array::TEXT FROM example_table.search('text_array:\"single element\"');")
            .expect("failed to query");
        assert_eq!(id, Some(3));
        assert_eq!(text_array, Some("{\"single element\"}".to_string()));
        assert_eq!(
            varchar_array,
            Some("{\"single varchar element\"}".to_string())
        );

        let (id, text_array, varchar_array) = Spi::get_three("SELECT id, text_array::TEXT, varchar_array::TEXT FROM example_table.search('varchar_array:varchar OR text_array:array') LIMIT 1;")
        .expect("failed to query");
        assert_eq!(id, Some(3));
        assert_eq!(text_array, Some("{\"single element\"}".to_string()));
        assert_eq!(
            varchar_array,
            Some("{\"single varchar element\"}".to_string())
        );

        let (id, text_array, varchar_array) = Spi::get_three("SELECT id, text_array::TEXT, varchar_array::TEXT FROM example_table.search('varchar_array:varchar OR text_array:array') LIMIT 1 OFFSET 1;")
        .expect("failed to query");
        assert_eq!(id, Some(2));
        assert_eq!(text_array, Some("{another,array,of,texts}".to_string()));
        assert_eq!(varchar_array, Some("{vtext3,vtext4,vtext5}".to_string()));
    }
}
