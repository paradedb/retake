use pgrx::pg_sys::{IndexBulkDeleteCallback, IndexBulkDeleteResult, ItemPointerData};
use pgrx::*;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::fs::{create_dir_all, remove_dir_all};
use std::path::Path;
use tantivy::{
    query::{Query, QueryParser},
    schema::*,
    DocAddress, Document, Index, IndexSettings, Score, Searcher, SingleSegmentIndexWriter, Term,
};

use crate::index_access::options::ParadeOptions;
use crate::json::builder::JsonBuilder;

const CACHE_NUM_BLOCKS: usize = 10;

pub struct TantivyScanState {
    pub schema: Schema,
    pub query: Box<dyn Query>,
    pub query_parser: QueryParser,
    pub searcher: Searcher,
    pub iterator: *mut std::vec::IntoIter<(Score, DocAddress)>,
}

pub struct ParadeIndex {
    fields: HashMap<String, Field>,
    underlying_index: Index,
}

impl ParadeIndex {
    pub fn new(name: String, table_name: String, options: PgBox<ParadeOptions>) -> Self {
        let dir = Self::get_data_directory(&name);
        let path = Path::new(&dir);
        if path.exists() {
            remove_dir_all(path).expect("failed to remove paradedb directory");
        }

        create_dir_all(path).expect("failed to create paradedb directory");

        let result = Self::build_index_schema(&table_name, options);
        let (schema, fields) = match result {
            Ok((s, f)) => (s, f),
            Err(e) => {
                panic!("Error building schema: {}", e);
            }
        };
        let settings = IndexSettings {
            docstore_compress_dedicated_thread: false, // Must run on single thread, or pgrx will panic
            ..Default::default()
        };

        let underlying_index = Index::builder()
            .schema(schema.clone())
            .settings(settings.clone())
            .create_in_dir(dir)
            .expect("failed to create index");

        Self {
            fields,
            underlying_index,
        }
    }

    pub fn from_index_name(name: String) -> Self {
        let dir = Self::get_data_directory(&name);

        let underlying_index = Index::open_in_dir(dir).expect("failed to open index");
        let schema = underlying_index.schema();

        let fields = schema
            .fields()
            .map(|field| {
                let (field, entry) = field;
                (entry.name().to_string(), field)
            })
            .collect();

        Self {
            fields,
            underlying_index,
        }
    }

    pub fn insert(
        &mut self,
        writer: &mut SingleSegmentIndexWriter,
        heap_tid: ItemPointerData,
        builder: JsonBuilder,
    ) {
        let mut doc: Document = Document::new();

        for (col_name, value) in builder.values {
            let field_option = self.fields.get(col_name.trim_matches('"'));

            if let Some(field) = field_option {
                value.add_to_tantivy_doc(&mut doc, field);
            }
        }

        let field_option = self.fields.get("heap_tid");
        doc.add_u64(*field_option.unwrap(), item_pointer_to_u64(heap_tid));
        writer.add_document(doc).expect("failed to add document");
    }

    pub fn bulk_delete(
        &self,
        stats_binding: *mut IndexBulkDeleteResult,
        callback: IndexBulkDeleteCallback,
        callback_state: *mut ::std::os::raw::c_void,
    ) {
        let mut index_writer = self.underlying_index.writer(50_000_000).unwrap(); // Adjust the size as necessary

        let schema = self.underlying_index.schema();
        let heap_tid_field = schema
            .get_field("heap_tid")
            .expect("Field 'heap_tid' not found in schema");

        let searcher = self
            .underlying_index
            .reader()
            .expect("Failed to acquire index reader")
            .searcher();

        for segment_reader in searcher.segment_readers() {
            let store_reader = segment_reader
                .get_store_reader(CACHE_NUM_BLOCKS)
                .expect("Failed to get store reader");

            for doc_id in 0..segment_reader.num_docs() {
                if let Ok(stored_fields) = store_reader.get(doc_id) {
                    if let Some(Value::I64(heap_tid_val)) = stored_fields.get_first(heap_tid_field)
                    {
                        if let Some(actual_callback) = callback {
                            let should_delete = unsafe {
                                actual_callback(
                                    *heap_tid_val as *mut ItemPointerData,
                                    callback_state,
                                )
                            };

                            if should_delete {
                                let term_to_delete =
                                    Term::from_field_i64(heap_tid_field, *heap_tid_val);
                                index_writer.delete_term(term_to_delete);
                                unsafe {
                                    (*stats_binding).tuples_removed += 1.0;
                                }
                            } else {
                                unsafe {
                                    (*stats_binding).num_index_tuples += 1.0;
                                }
                            }
                        }
                    }
                }
            }
        }

        index_writer.commit().unwrap();
    }

    pub fn scan(&self) -> TantivyScanState {
        let schema = self.underlying_index.schema();
        let reader = self
            .underlying_index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()
            .expect("failed to create index reader");

        let searcher = reader.searcher();

        let query_parser = QueryParser::for_index(
            &self.underlying_index,
            schema.fields().map(|(field, _)| field).collect::<Vec<_>>(),
        );
        let empty_query = query_parser.parse_query("").unwrap();

        TantivyScanState {
            schema,
            query: empty_query,
            query_parser,
            searcher,
            iterator: std::ptr::null_mut(),
        }
    }

    pub fn copy_tantivy_index(&self) -> tantivy::Index {
        self.underlying_index.clone()
    }

    fn get_data_directory(name: &str) -> String {
        unsafe {
            let option_name_cstr =
                CString::new("data_directory").expect("failed to create CString");
            let data_dir_str = String::from_utf8(
                CStr::from_ptr(pg_sys::GetConfigOptionByName(
                    option_name_cstr.as_ptr(),
                    std::ptr::null_mut(),
                    true,
                ))
                .to_bytes()
                .to_vec(),
            )
            .expect("Failed to convert C string to Rust string");

            format!("{}/{}/{}", data_dir_str, "paradedb", name)
        }
    }

    fn build_index_schema(
        name: &str,
        options: PgBox<ParadeOptions>,
    ) -> Result<(Schema, HashMap<String, Field>), String> {
        let indexrel = unsafe {
            PgRelation::open_with_name(name)
                .unwrap_or_else(|_| panic!("failed to open relation {}", name))
        };
        let field_options = options.get_fields();

        let tupdesc = indexrel.tuple_desc();
        let attributes: HashMap<String, PgOid> = tupdesc
            .iter()
            .filter(|&attribute| !attribute.is_dropped())
            .map(|attribute| (attribute.name().to_string(), attribute.type_oid()))
            .collect();

        let mut schema_builder = Schema::builder();
        let mut tantivy_fields: HashMap<String, Field> = HashMap::new();

        if let Some(field_options_map) = field_options.as_object() {
            for (field_name, options) in field_options_map {
                assert!(
                    attributes.contains_key(field_name),
                    "{} is not a recognized column name",
                    field_name
                );
                let attribute_type_oid = attributes.get(field_name).unwrap();

                let field = match &attribute_type_oid {
                    PgOid::BuiltIn(builtin) => match builtin {
                        PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID => {
                            let text_options = Self::build_text_options(options, field_name)
                                .expect("failed to build TextOptions");
                            Some(schema_builder.add_text_field(field_name, text_options))
                        }
                        PgBuiltInOids::JSONOID | PgBuiltInOids::JSONBOID => {
                            Some(schema_builder.add_json_field(field_name, STORED))
                        }
                        _ => None,
                    },
                    _ => None,
                };

                if let Some(valid_field) = field {
                    tantivy_fields.insert(field_name.clone(), valid_field);
                }
            }
        }

        let field = schema_builder.add_u64_field("heap_tid", INDEXED | STORED);
        tantivy_fields.insert("heap_tid".to_string(), field);

        Ok((schema_builder.build(), tantivy_fields))
    }

    fn build_text_options(
        json_value: &serde_json::Value,
        field_name: &str,
    ) -> Result<TextOptions, String> {
        let mut text_options = TextOptions::default();

        if let Some(obj) = json_value.as_object() {
            if let Some(fast) = obj.get("fast").and_then(&serde_json::Value::as_bool) {
                if fast {
                    text_options = text_options.set_fast(None);
                } else {
                    text_options = text_options.set_stored();
                }
            }

            if let Some(tokenizer) = obj.get("tokenizer").and_then(serde_json::Value::as_str) {
                text_options = text_options
                    .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
            }

            if let Some(index_option) = obj.get("index_option").and_then(serde_json::Value::as_str)
            {
                text_options = text_options.set_indexing_options(
                    TextFieldIndexing::default().set_index_option(IndexRecordOption::Basic),
                );
            }

            Ok(text_options)
        } else {
            return Err(format!("options for {} is not an object", field_name));
        }
    }
}
