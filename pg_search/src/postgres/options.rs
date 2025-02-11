// Copyright (c) 2023-2025 Retake, Inc.
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

use anyhow::Result;
use memoffset::*;
use pgrx::pg_sys::AsPgCStr;
use pgrx::*;
use serde_json::{json, Map};
use std::collections::HashMap;
use std::ffi::CStr;
use tokenizers::{manager::SearchTokenizerFilters, SearchNormalizer, SearchTokenizer};

use crate::schema::{IndexRecordOption, SearchFieldConfig, SearchFieldName, SearchFieldType};

/* ADDING OPTIONS
 * in init(), call pg_sys::add_{type}_reloption (check postgres docs for what args you need)
 * add the corresponding entries to SearchIndexCreateOptions struct definition
 * in amoptions(), add a relopt_parse_elt entry to the options array and change NUM_REL_OPTS
 * Note that for string options, postgres will give you the offset of the string, and you have to read the string
 * yourself (see get_tokenizer)
*/

/* READING OPTIONS
 * options are placed in relation.rd_options
 * As in ambuild(), cast relation.rd_options into SearchIndexCreateOptions using PgBox
 * (because SearchIndexCreateOptions is a postgres-allocated object) and use getters and setters
*/

static mut RELOPT_KIND_PDB: pg_sys::relopt_kind::Type = 0;

// Postgres handles string options by placing each option offset bytes from the start of rdopts and
// plops the offset in the struct
#[repr(C)]
pub struct SearchIndexCreateOptions {
    // varlena header (needed bc postgres treats this as bytea)
    vl_len_: i32,
    text_fields_offset: i32,
    numeric_fields_offset: i32,
    boolean_fields_offset: i32,
    json_fields_offset: i32,
    range_fields_offset: i32,
    datetime_fields_offset: i32,
    key_field_offset: i32,
}

#[pg_guard]
extern "C" fn validate_text_fields(value: *const std::os::raw::c_char) {
    let json_str = cstr_to_rust_str(value);
    if json_str.is_empty() {
        return;
    }
    SearchIndexCreateOptions::deserialize_config_fields(
        json_str,
        &SearchFieldConfig::text_from_json,
    );
}

#[pg_guard]
extern "C" fn validate_numeric_fields(value: *const std::os::raw::c_char) {
    let json_str = cstr_to_rust_str(value);
    if json_str.is_empty() {
        return;
    }
    SearchIndexCreateOptions::deserialize_config_fields(
        json_str,
        &SearchFieldConfig::numeric_from_json,
    );
}

#[pg_guard]
extern "C" fn validate_boolean_fields(value: *const std::os::raw::c_char) {
    let json_str = cstr_to_rust_str(value);
    if json_str.is_empty() {
        return;
    }
    SearchIndexCreateOptions::deserialize_config_fields(
        json_str,
        &SearchFieldConfig::boolean_from_json,
    );
}

#[pg_guard]
extern "C" fn validate_json_fields(value: *const std::os::raw::c_char) {
    let json_str = cstr_to_rust_str(value);
    if json_str.is_empty() {
        return;
    }
    SearchIndexCreateOptions::deserialize_config_fields(
        json_str,
        &SearchFieldConfig::json_from_json,
    );
}

#[pg_guard]
extern "C" fn validate_range_fields(value: *const std::os::raw::c_char) {
    let json_str = cstr_to_rust_str(value);
    if json_str.is_empty() {
        return;
    }
    SearchIndexCreateOptions::deserialize_config_fields(
        json_str,
        &SearchFieldConfig::range_from_json,
    );
}

#[pg_guard]
extern "C" fn validate_datetime_fields(value: *const std::os::raw::c_char) {
    let json_str = cstr_to_rust_str(value);
    if json_str.is_empty() {
        return;
    }
    SearchIndexCreateOptions::deserialize_config_fields(
        json_str,
        &SearchFieldConfig::date_from_json,
    );
}

#[pg_guard]
extern "C" fn validate_fields(value: *const std::os::raw::c_char) {
    let json_str = cstr_to_rust_str(value);
    if json_str.is_empty() {
        return;
    }

    // Just ensure the config can be deserialized as json.
    let _: HashMap<String, serde_json::Value> = json5::from_str(&json_str)
        .unwrap_or_else(|err| panic!("failed to deserialize field config: {err:?}"));
}

#[pg_guard]
extern "C" fn validate_key_field(value: *const std::os::raw::c_char) {
    cstr_to_rust_str(value);
}

#[inline]
fn cstr_to_rust_str(value: *const std::os::raw::c_char) -> String {
    if value.is_null() {
        return "".to_string();
    }

    unsafe { CStr::from_ptr(value) }
        .to_str()
        .expect("failed to parse fields as utf-8")
        .to_string()
}

const NUM_REL_OPTS: usize = 7;
#[pg_guard]
pub unsafe extern "C" fn amoptions(
    reloptions: pg_sys::Datum,
    validate: bool,
) -> *mut pg_sys::bytea {
    let options: [pg_sys::relopt_parse_elt; NUM_REL_OPTS] = [
        pg_sys::relopt_parse_elt {
            optname: "text_fields".as_pg_cstr(),
            opttype: pg_sys::relopt_type::RELOPT_TYPE_STRING,
            offset: offset_of!(SearchIndexCreateOptions, text_fields_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "numeric_fields".as_pg_cstr(),
            opttype: pg_sys::relopt_type::RELOPT_TYPE_STRING,
            offset: offset_of!(SearchIndexCreateOptions, numeric_fields_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "boolean_fields".as_pg_cstr(),
            opttype: pg_sys::relopt_type::RELOPT_TYPE_STRING,
            offset: offset_of!(SearchIndexCreateOptions, boolean_fields_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "json_fields".as_pg_cstr(),
            opttype: pg_sys::relopt_type::RELOPT_TYPE_STRING,
            offset: offset_of!(SearchIndexCreateOptions, json_fields_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "range_fields".as_pg_cstr(),
            opttype: pg_sys::relopt_type::RELOPT_TYPE_STRING,
            offset: offset_of!(SearchIndexCreateOptions, range_fields_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "datetime_fields".as_pg_cstr(),
            opttype: pg_sys::relopt_type::RELOPT_TYPE_STRING,
            offset: offset_of!(SearchIndexCreateOptions, datetime_fields_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "key_field".as_pg_cstr(),
            opttype: pg_sys::relopt_type::RELOPT_TYPE_STRING,
            offset: offset_of!(SearchIndexCreateOptions, key_field_offset) as i32,
        },
    ];
    build_relopts(reloptions, validate, options)
}

unsafe fn build_relopts(
    reloptions: pg_sys::Datum,
    validate: bool,
    options: [pg_sys::relopt_parse_elt; NUM_REL_OPTS],
) -> *mut pg_sys::bytea {
    let rdopts = pg_sys::build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND_PDB,
        std::mem::size_of::<SearchIndexCreateOptions>(), // TODO: proper size calculator
        options.as_ptr(),
        NUM_REL_OPTS as i32,
    );

    rdopts as *mut pg_sys::bytea
}

impl SearchIndexCreateOptions {
    /// As a SearchFieldConfig is an enum, for it to be correctly serialized the variant needs
    /// to be present on the json object. This helper method will "wrap" the json object in
    /// another object with the variant key, which is passed into the function. For example:
    ///
    /// {"Text": { <actual_config> }}
    ///
    /// This way, serde will know to deserialize the config as SearchFieldConfig::Text.
    fn deserialize_config_fields(
        serialized: String,
        parser: &dyn Fn(serde_json::Value) -> Result<SearchFieldConfig>,
    ) -> Vec<(SearchFieldName, SearchFieldConfig)> {
        let config_map: Map<String, serde_json::Value> = serde_json::from_str(&serialized)
            .unwrap_or_else(|err| panic!("failed to deserialize field config: {err:?}"));

        config_map
            .into_iter()
            .map(|(field_name, field_config)| {
                (
                    field_name.clone().into(),
                    parser(field_config)
                        .expect("field config should be valid for SearchFieldConfig::{field_name}"),
                )
            })
            .collect()
    }

    pub fn get_text_fields(&self) -> Vec<(SearchFieldName, SearchFieldConfig)> {
        let config = self.get_str(self.text_fields_offset, "".to_string());
        if config.is_empty() {
            return Vec::new();
        }
        Self::deserialize_config_fields(config, &SearchFieldConfig::text_from_json)
    }

    pub fn get_numeric_fields(&self) -> Vec<(SearchFieldName, SearchFieldConfig)> {
        let config = self.get_str(self.numeric_fields_offset, "".to_string());
        if config.is_empty() {
            return Vec::new();
        }
        Self::deserialize_config_fields(config, &SearchFieldConfig::numeric_from_json)
    }

    pub fn get_boolean_fields(&self) -> Vec<(SearchFieldName, SearchFieldConfig)> {
        let config = self.get_str(self.boolean_fields_offset, "".to_string());
        if config.is_empty() {
            return Vec::new();
        }
        Self::deserialize_config_fields(config, &SearchFieldConfig::boolean_from_json)
    }

    pub fn get_json_fields(&self) -> Vec<(SearchFieldName, SearchFieldConfig)> {
        let config = self.get_str(self.json_fields_offset, "".to_string());
        if config.is_empty() {
            return Vec::new();
        }
        Self::deserialize_config_fields(config, &SearchFieldConfig::json_from_json)
    }

    pub fn get_range_fields(&self) -> Vec<(SearchFieldName, SearchFieldConfig)> {
        let config = self.get_str(self.range_fields_offset, "".to_string());
        if config.is_empty() {
            return Vec::new();
        }
        Self::deserialize_config_fields(config, &SearchFieldConfig::range_from_json)
    }

    pub fn get_datetime_fields(&self) -> Vec<(SearchFieldName, SearchFieldConfig)> {
        let config = self.get_str(self.datetime_fields_offset, "".to_string());
        if config.is_empty() {
            return Vec::new();
        }
        Self::deserialize_config_fields(config, &SearchFieldConfig::date_from_json)
    }

    fn json_value_to_search_field_config(
        field_type: &SearchFieldType,
        field_config: serde_json::Value,
    ) -> SearchFieldConfig {
        match field_type {
            SearchFieldType::Text => SearchFieldConfig::text_from_json(field_config),
            SearchFieldType::I64 => SearchFieldConfig::numeric_from_json(field_config),
            SearchFieldType::F64 => SearchFieldConfig::numeric_from_json(field_config),
            SearchFieldType::U64 => SearchFieldConfig::numeric_from_json(field_config),
            SearchFieldType::Bool => SearchFieldConfig::boolean_from_json(field_config),
            SearchFieldType::Json => SearchFieldConfig::json_from_json(field_config),
            SearchFieldType::Date => SearchFieldConfig::date_from_json(field_config),
            SearchFieldType::Range => SearchFieldConfig::range_from_json(field_config),
        }
        .expect("field config should be valid for SearchFieldConfig::{field_name}")
    }

    pub fn get_key_field(&self) -> Option<SearchFieldName> {
        let key_field_name = self.get_str(self.key_field_offset, "".to_string());
        if key_field_name.is_empty() {
            return None;
        }
        Some(SearchFieldName(key_field_name))
    }

    fn get_key_field_config(
        &self,
        heaprel: &PgRelation,
    ) -> (SearchFieldName, SearchFieldConfig, SearchFieldType) {
        // Create a map from column name to column type. We'll use this to verify that index
        // configurations passed by the user reference the correct types for each column.
        let name_type_map: HashMap<SearchFieldName, SearchFieldType> = heaprel
            .tuple_desc()
            .into_iter()
            .filter_map(|attribute| {
                let attname = attribute.name();
                let attribute_type_oid = attribute.type_oid();
                let array_type = unsafe { pg_sys::get_element_type(attribute_type_oid.value()) };
                let base_oid = if array_type != pg_sys::InvalidOid {
                    PgOid::from(array_type)
                } else {
                    attribute_type_oid
                };
                if let Ok(search_field_type) = SearchFieldType::try_from(&base_oid) {
                    Some((attname.into(), search_field_type))
                } else {
                    None
                }
            })
            .collect();

        let key_field_name = self.get_key_field().expect("must specify key_field");
        let key_field_type = match name_type_map.get(&key_field_name) {
            Some(field_type) => field_type,
            None => panic!("key field does not exist"),
        };
        let key_field_config = match key_field_type {
            SearchFieldType::I64 | SearchFieldType::U64 | SearchFieldType::F64 => {
                SearchFieldConfig::Numeric {
                    indexed: true,
                    fast: true,
                    stored: true,
                    column: None,
                }
            }
            SearchFieldType::Text => SearchFieldConfig::Text {
                indexed: true,
                fast: true,
                stored: true,
                fieldnorms: false,
                tokenizer: SearchTokenizer::Raw(SearchTokenizerFilters::default()),
                record: IndexRecordOption::Basic,
                normalizer: SearchNormalizer::Raw,
                column: None,
            },
            SearchFieldType::Json => SearchFieldConfig::Json {
                indexed: true,
                fast: true,
                stored: true,
                fieldnorms: false,
                expand_dots: false,
                tokenizer: SearchTokenizer::Raw(SearchTokenizerFilters::default()),
                record: IndexRecordOption::Basic,
                normalizer: SearchNormalizer::Raw,
                column: None,
                nested: None,
            },
            SearchFieldType::Range => SearchFieldConfig::Range {
                stored: true,
                column: None,
            },
            SearchFieldType::Bool => SearchFieldConfig::Boolean {
                indexed: true,
                fast: true,
                stored: true,
                column: None,
            },
            SearchFieldType::Date => SearchFieldConfig::Date {
                indexed: true,
                fast: true,
                stored: true,
                column: None,
            },
        };

        (key_field_name, key_field_config, *key_field_type)
    }

    pub fn get_fields(
        &self,
        heaprel: &PgRelation,
        index_info: *mut pg_sys::IndexInfo,
    ) -> Vec<(SearchFieldName, SearchFieldConfig, SearchFieldType)> {
        let tupdesc = heaprel.tuple_desc();
        let (key_field_name, key_field_config, key_field_type) = self.get_key_field_config(heaprel);

        let mut config_by_name = [
            self.text_fields_offset,
            self.numeric_fields_offset,
            self.boolean_fields_offset,
            self.json_fields_offset,
            self.range_fields_offset,
            self.datetime_fields_offset,
        ]
        .into_iter()
        .map(|offset| self.get_str(offset, "".to_string()))
        .filter(|config| !config.is_empty())
        .flat_map(|config| {
            serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&config)
                .unwrap_or_else(|err| panic!("error in JSON field config: {err}: {config}"))
                .into_iter()
        })
        .collect::<HashMap<_, _>>();

        let num_index_attrs = unsafe { (*index_info).ii_NumIndexAttrs };
        let mut fields_by_name = (0..num_index_attrs)
            .map(|i| {
                let attr_number = unsafe { (*index_info).ii_IndexAttrNumbers[i as usize] };
                let attribute = tupdesc
                    .get((attr_number - 1) as usize)
                    .expect("attribute should exist");
                let column_name = attribute.name();
                let column_type_oid = attribute.type_oid();

                let array_type = unsafe { pg_sys::get_element_type(column_type_oid.value()) };
                let base_oid = if array_type != pg_sys::InvalidOid {
                    PgOid::from(array_type)
                } else {
                    column_type_oid
                };

                let field_type = SearchFieldType::try_from(&base_oid).unwrap_or_else(|err| {
                    panic!("cannot index column '{column_name}' with type {base_oid:?}: {err}")
                });

                if column_name == key_field_name.0 && config_by_name.contains_key(column_name){
                    panic!("cannot override BM25 configuration for key_field '{column_name}', you must use an aliased field name and 'column' configuration key");
                }

                let json_config = config_by_name
                    .remove(column_name)
                    .unwrap_or_else(|| json!({}));

                (
                    column_name.to_string(),
                    (
                        column_name.into(),
                        Self::json_value_to_search_field_config(&field_type, json_config),
                        field_type,
                    ),
                )
            })
            .collect::<HashMap<_, _>>();

        // Ensure the key_field entry has the correct default values.
        fields_by_name.insert(
            key_field_name.0.clone(),
            (key_field_name, key_field_config, key_field_type),
        );

        // Iterate through all the configured fields to check for fields configured that don't
        // have a matching Postgres column (for features like multiple tokenizers).
        // Above, we've mutated config_by_name and removed entries that have a matching
        // Postgres column, so all the entries below are aliases.
        for (name, json_config) in config_by_name {
            // A field not corresponding to a Postgres table column MUST have a 'column' key
            // on the configuration, telling us which column contains the data to index.
            if let Some(column) = json_config.get("column").and_then(|c| c.as_str()) {
                if let Some((_, _, field_type)) = fields_by_name.get(column) {
                    fields_by_name.insert(
                        name.to_string(),
                        (
                            SearchFieldName(name.to_string()),
                            Self::json_value_to_search_field_config(field_type, json_config),
                            *field_type,
                        ),
                    );
                }
            } else {
                panic!("Field '{name}' does not match any column, and has no 'column' key")
            }
        }

        fields_by_name.into_values().collect()
    }

    fn get_str(&self, offset: i32, default: String) -> String {
        if offset == 0 {
            default
        } else {
            let opts = self as *const _ as void_ptr as usize;
            let value =
                unsafe { CStr::from_ptr((opts + offset as usize) as *const std::os::raw::c_char) };

            value
                .to_str()
                .expect("value should be valid utf-8")
                .to_owned()
        }
    }
}

// it adds the tokenizer option to the list of relation options so we can parse it in amoptions
pub unsafe fn init() {
    // adding our own relopt type because zombodb does, but one of the built-in Postgres ones might be more appropriate
    RELOPT_KIND_PDB = pg_sys::add_reloption_kind();
    pg_sys::add_string_reloption(
        RELOPT_KIND_PDB,
        "text_fields".as_pg_cstr(),
        "JSON string specifying how text fields should be indexed".as_pg_cstr(),
        std::ptr::null(),
        Some(validate_text_fields),
        pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_PDB,
        "numeric_fields".as_pg_cstr(),
        "JSON string specifying how numeric fields should be indexed".as_pg_cstr(),
        std::ptr::null(),
        Some(validate_numeric_fields),
        pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_PDB,
        "boolean_fields".as_pg_cstr(),
        "JSON string specifying how boolean fields should be indexed".as_pg_cstr(),
        std::ptr::null(),
        Some(validate_boolean_fields),
        pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_PDB,
        "json_fields".as_pg_cstr(),
        "JSON string specifying how JSON fields should be indexed".as_pg_cstr(),
        std::ptr::null(),
        Some(validate_json_fields),
        pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_PDB,
        "range_fields".as_pg_cstr(),
        "JSON string specifying how range fields should be indexed".as_pg_cstr(),
        std::ptr::null(),
        Some(validate_range_fields),
        pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_PDB,
        "datetime_fields".as_pg_cstr(),
        "JSON string specifying how date fields should be indexed".as_pg_cstr(),
        std::ptr::null(),
        Some(validate_datetime_fields),
        pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_PDB,
        "key_field".as_pg_cstr(),
        "Column name as a string specify the unique identifier for a row".as_pg_cstr(),
        std::ptr::null(),
        Some(validate_key_field),
        pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
    );
}
