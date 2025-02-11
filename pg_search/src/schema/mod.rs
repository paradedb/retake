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

mod anyenum;
mod document;
pub mod range;

use anyhow::{Context, Result};
use derive_more::{AsRef, Display, From, Into};
pub use document::*;
use pgrx::{PgBuiltInOids, PgOid, PgRelation};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use tantivy::schema::{
    DateOptions, DateTimePrecision, Field, JsonObjectOptions, NumericOptions, Schema,
    TextFieldIndexing, TextOptions,
};
use thiserror::Error;
use tokenizers::{SearchNormalizer, SearchTokenizer};

use crate::postgres::index::get_fields;
use crate::query::AsFieldType;
pub use anyenum::AnyEnum;

/// The id of a field, stored in the index.
#[derive(Debug, Clone, Display, From, AsRef, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[from(forward)]
pub struct SearchFieldName(pub String);

/// The name of a field, as it appears to Postgres.
#[derive(Debug, Copy, Clone, From, PartialEq, Eq, Serialize, Deserialize)]
#[from(forward)]
pub struct SearchFieldId(pub Field);

/// The name of the index, as it appears to Postgres.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchIndexName(pub String);
/// The type of the search field.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SearchFieldType {
    Text,
    I64,
    F64,
    U64,
    Bool,
    Json,
    Date,
    Range,
}

impl TryFrom<&PgOid> for SearchFieldType {
    type Error = SearchIndexSchemaError;
    fn try_from(pg_oid: &PgOid) -> Result<Self, Self::Error> {
        match &pg_oid {
            PgOid::BuiltIn(builtin) => match builtin {
                PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID | PgBuiltInOids::UUIDOID => {
                    Ok(SearchFieldType::Text)
                }
                PgBuiltInOids::INT2OID | PgBuiltInOids::INT4OID | PgBuiltInOids::INT8OID => {
                    Ok(SearchFieldType::I64)
                }
                PgBuiltInOids::OIDOID | PgBuiltInOids::XIDOID => Ok(SearchFieldType::U64),
                PgBuiltInOids::FLOAT4OID | PgBuiltInOids::FLOAT8OID | PgBuiltInOids::NUMERICOID => {
                    Ok(SearchFieldType::F64)
                }
                PgBuiltInOids::BOOLOID => Ok(SearchFieldType::Bool),
                PgBuiltInOids::JSONOID | PgBuiltInOids::JSONBOID => Ok(SearchFieldType::Json),
                PgBuiltInOids::INT4RANGEOID
                | PgBuiltInOids::INT8RANGEOID
                | PgBuiltInOids::NUMRANGEOID
                | PgBuiltInOids::DATERANGEOID
                | PgBuiltInOids::TSRANGEOID
                | PgBuiltInOids::TSTZRANGEOID => Ok(SearchFieldType::Range),
                PgBuiltInOids::DATEOID
                | PgBuiltInOids::TIMESTAMPOID
                | PgBuiltInOids::TIMESTAMPTZOID
                | PgBuiltInOids::TIMEOID
                | PgBuiltInOids::TIMETZOID => Ok(SearchFieldType::Date),
                _ => Err(SearchIndexSchemaError::InvalidPgOid(*pg_oid)),
            },
            PgOid::Custom(custom) => {
                if unsafe { pgrx::pg_sys::type_is_enum(*custom) } {
                    Ok(SearchFieldType::F64)
                } else {
                    Err(SearchIndexSchemaError::InvalidPgOid(*pg_oid))
                }
            }
            _ => Err(SearchIndexSchemaError::InvalidPgOid(*pg_oid)),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub enum SearchFieldConfig {
    Text {
        #[serde(default = "default_as_true")]
        indexed: bool,
        #[serde(default)]
        fast: bool,
        #[serde(default = "default_as_false")]
        stored: bool,
        #[serde(default = "default_as_true")]
        fieldnorms: bool,
        #[serde(default)]
        tokenizer: SearchTokenizer,
        #[serde(default = "default_as_freqs_and_positions")]
        record: IndexRecordOption,
        #[serde(default)]
        normalizer: SearchNormalizer,
        #[serde(default)]
        column: Option<String>,
    },
    Json {
        #[serde(default = "default_as_true")]
        indexed: bool,
        #[serde(default)]
        fast: bool,
        #[serde(default = "default_as_false")]
        stored: bool,
        #[serde(default = "default_as_true")]
        fieldnorms: bool,
        #[serde(default = "default_as_true")]
        expand_dots: bool,
        #[serde(default)]
        tokenizer: SearchTokenizer,
        #[serde(default = "default_as_freqs_and_positions")]
        record: IndexRecordOption,
        #[serde(default)]
        normalizer: SearchNormalizer,
        #[serde(default)]
        column: Option<String>,
        #[serde(default)]
        nested: Option<serde_json::Value>,
    },
    Range {
        #[serde(default = "default_as_false")]
        stored: bool,
        #[serde(default)]
        column: Option<String>,
    },
    Numeric {
        #[serde(default = "default_as_true")]
        indexed: bool,
        #[serde(default = "default_as_true")]
        fast: bool,
        #[serde(default = "default_as_false")]
        stored: bool,
        #[serde(default)]
        column: Option<String>,
    },
    Boolean {
        #[serde(default = "default_as_true")]
        indexed: bool,
        #[serde(default = "default_as_true")]
        fast: bool,
        #[serde(default = "default_as_false")]
        stored: bool,
        #[serde(default)]
        column: Option<String>,
    },
    Date {
        #[serde(default = "default_as_true")]
        indexed: bool,
        #[serde(default = "default_as_true")]
        fast: bool,
        #[serde(default = "default_as_false")]
        stored: bool,
        #[serde(default)]
        column: Option<String>,
    },
}

impl SearchFieldConfig {
    pub fn text_from_json(value: serde_json::Value) -> Result<Self> {
        let obj = value
            .as_object()
            .context("Expected a JSON object for Text configuration")?;

        let indexed = match obj.get("indexed") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'indexed' field should be a boolean")),
            None => Ok(true),
        }?;

        let fast = match obj.get("fast") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'fast' field should be a boolean")),
            None => Ok(false),
        }?;

        let stored = match obj.get("stored") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'stored' field should be a boolean")),
            None => Ok(false),
        }?;

        let fieldnorms = match obj.get("fieldnorms") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'fieldnorms' field should be a boolean")),
            None => Ok(true),
        }?;

        let tokenizer = match obj.get("tokenizer") {
            Some(v) => SearchTokenizer::from_json_value(v),
            None => Ok(SearchTokenizer::default()),
        }?;

        let record = match obj.get("record") {
            Some(v) => serde_json::from_value(v.clone()),
            None => Ok(default_as_freqs_and_positions()),
        }?;

        let normalizer = match obj.get("normalizer") {
            Some(v) => serde_json::from_value(v.clone()),
            None => Ok(SearchNormalizer::Raw),
        }?;

        let column = match obj.get("column") {
            Some(v) => v
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("'column' field should be a string"))
                .map(|s| Some(s.to_string())),
            None => Ok(None),
        }?;

        Ok(SearchFieldConfig::Text {
            indexed,
            fast,
            stored,
            fieldnorms,
            tokenizer,
            record,
            normalizer,
            column,
        })
    }

    pub fn json_from_json(value: serde_json::Value) -> Result<Self> {
        let obj = value
            .as_object()
            .context("Expected a JSON object for Json configuration")?;

        let indexed = match obj.get("indexed") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'indexed' field should be a boolean")),
            None => Ok(true),
        }?;

        let fast = match obj.get("fast") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'fast' field should be a boolean")),
            None => Ok(false),
        }?;

        let stored = match obj.get("stored") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'stored' field should be a boolean")),
            None => Ok(false),
        }?;

        let expand_dots = match obj.get("expand_dots") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'expand_dots' field should be a boolean")),
            None => Ok(true),
        }?;

        let tokenizer = match obj.get("tokenizer") {
            Some(v) => SearchTokenizer::from_json_value(v),
            None => Ok(SearchTokenizer::default()),
        }?;

        let record = match obj.get("record") {
            Some(v) => serde_json::from_value(v.clone()),
            None => Ok(default_as_freqs_and_positions()),
        }?;

        let normalizer = match obj.get("normalizer") {
            Some(v) => serde_json::from_value(v.clone()),
            None => Ok(SearchNormalizer::Raw),
        }?;

        let fieldnorms = match obj.get("fieldnorms") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'fieldnorms' field should be a boolean")),
            None => Ok(true),
        }?;

        let column = match obj.get("column") {
            Some(v) => v
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("'column' field should be a string"))
                .map(|s| Some(s.to_string())),
            None => Ok(None),
        }?;

        let nested = match obj.get("nested") {
            Some(v) => serde_json::from_value(v.clone()),
            None => Ok(None),
        }?;

        Ok(SearchFieldConfig::Json {
            indexed,
            fast,
            stored,
            fieldnorms,
            expand_dots,
            tokenizer,
            record,
            normalizer,
            column,
            nested,
        })
    }

    pub fn range_from_json(value: serde_json::Value) -> Result<Self> {
        let obj = value
            .as_object()
            .context("Expected a JSON object for Json configuration")?;

        let stored = match obj.get("stored") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'stored' field should be a boolean")),
            None => Ok(false),
        }?;

        let column = match obj.get("column") {
            Some(v) => v
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("'column' field should be a string"))
                .map(|s| Some(s.to_string())),
            None => Ok(None),
        }?;

        Ok(SearchFieldConfig::Range { stored, column })
    }

    pub fn numeric_from_json(value: serde_json::Value) -> Result<Self> {
        let obj = value
            .as_object()
            .context("Expected a JSON object for Numeric configuration")?;

        let indexed = match obj.get("indexed") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'indexed' field should be a boolean")),
            None => Ok(true),
        }?;

        let fast = match obj.get("fast") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'fast' field should be a boolean")),
            None => Ok(true),
        }?;

        let stored = match obj.get("stored") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'stored' field should be a boolean")),
            None => Ok(false),
        }?;

        let column = match obj.get("column") {
            Some(v) => v
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("'column' field should be a string"))
                .map(|s| Some(s.to_string())),
            None => Ok(None),
        }?;

        Ok(SearchFieldConfig::Numeric {
            indexed,
            fast,
            stored,
            column,
        })
    }

    pub fn boolean_from_json(value: serde_json::Value) -> Result<Self> {
        let obj = value
            .as_object()
            .context("Expected a JSON object for Boolean configuration")?;

        let indexed = match obj.get("indexed") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'indexed' field should be a boolean")),
            None => Ok(true),
        }?;

        let fast = match obj.get("fast") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'fast' field should be a boolean")),
            None => Ok(true),
        }?;

        let stored = match obj.get("stored") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'stored' field should be a boolean")),
            None => Ok(false),
        }?;

        let column = match obj.get("column") {
            Some(v) => v
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("'column' field should be a string"))
                .map(|s| Some(s.to_string())),
            None => Ok(None),
        }?;

        Ok(SearchFieldConfig::Boolean {
            indexed,
            fast,
            stored,
            column,
        })
    }

    pub fn date_from_json(value: serde_json::Value) -> Result<Self> {
        let obj = value
            .as_object()
            .context("Expected a JSON object for Date configuration")?;

        let indexed = match obj.get("indexed") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'indexed' field should be a boolean")),
            None => Ok(true),
        }?;

        let fast = match obj.get("fast") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'fast' field should be a boolean")),
            None => Ok(true),
        }?;

        let stored = match obj.get("stored") {
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("'stored' field should be a boolean")),
            None => Ok(false),
        }?;

        let column = match obj.get("column") {
            Some(v) => v
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("'column' field should be a string"))
                .map(|s| Some(s.to_string())),
            None => Ok(None),
        }?;

        Ok(SearchFieldConfig::Date {
            indexed,
            fast,
            stored,
            column,
        })
    }

    pub fn column(&self) -> Option<&String> {
        match self {
            Self::Text { column, .. }
            | Self::Json { column, .. }
            | Self::Range { column, .. }
            | Self::Numeric { column, .. }
            | Self::Boolean { column, .. }
            | Self::Date { column, .. } => column.as_ref(),
        }
    }

    pub fn is_nested(&self) -> bool {
        match self {
            SearchFieldConfig::Json { nested, .. } => nested.is_some(),
            _ => false,
        }
    }
}

impl SearchFieldConfig {
    pub fn from_json(value: serde_json::Value) -> Self {
        serde_json::from_value(value)
            .expect("value should be a valid SearchFieldConfig representation")
    }

    pub fn default_text() -> Self {
        Self::from_json(json!({"Text": {}}))
    }

    pub fn default_numeric() -> Self {
        Self::from_json(json!({"Numeric": {}}))
    }

    pub fn default_boolean() -> Self {
        Self::from_json(json!({"Boolean": {}}))
    }

    pub fn default_json() -> Self {
        Self::from_json(json!({"Json": {}}))
    }

    pub fn default_date() -> Self {
        Self::from_json(json!({"Date": {}}))
    }
}

impl From<SearchFieldConfig> for TextOptions {
    fn from(config: SearchFieldConfig) -> Self {
        let mut text_options = TextOptions::default();
        match config {
            SearchFieldConfig::Text {
                indexed,
                fast,
                stored,
                fieldnorms,
                tokenizer,
                record,
                normalizer,
                ..
            } => {
                if stored {
                    text_options = text_options.set_stored();
                }
                if fast {
                    text_options = text_options.set_fast(Some(normalizer.name()));
                }
                if indexed {
                    let text_field_indexing = TextFieldIndexing::default()
                        .set_index_option(record.into())
                        .set_fieldnorms(fieldnorms)
                        .set_tokenizer(&tokenizer.name());

                    text_options = text_options.set_indexing_options(text_field_indexing);
                }
            }
            _ => panic!("attempted to convert non-text search field config to tantivy text config"),
        }
        text_options
    }
}

impl From<SearchFieldConfig> for NumericOptions {
    fn from(config: SearchFieldConfig) -> Self {
        let mut numeric_options = NumericOptions::default();
        match config {
            SearchFieldConfig::Numeric {
                indexed,
                fast,
                stored,
                ..
            }
            // Following the example of Quickwit, which uses NumericOptions for boolean options.
            | SearchFieldConfig::Boolean { indexed, fast, stored, .. } => {
                if stored {
                    numeric_options = numeric_options.set_stored();
                }
                if fast {
                    numeric_options = numeric_options.set_fast();
                }
                if indexed {
                    numeric_options = numeric_options.set_indexed();
                }
            }
            _ => {
                panic!(
                    "attempted to convert non-numeric search field config to tantivy numeric config"
                )
            }
        }
        numeric_options
    }
}

impl From<SearchFieldConfig> for JsonObjectOptions {
    fn from(config: SearchFieldConfig) -> Self {
        let mut json_options = JsonObjectOptions::default();
        match config {
            SearchFieldConfig::Json {
                indexed,
                fast,
                stored,
                fieldnorms,
                expand_dots,
                tokenizer,
                record,
                normalizer,
                nested,
                ..
            } => {
                if stored {
                    json_options = json_options.set_stored();
                }
                if fast {
                    json_options = json_options.set_fast(Some(normalizer.name()));
                }
                if expand_dots {
                    json_options = json_options.set_expand_dots_enabled();
                }
                if indexed {
                    let text_field_indexing = TextFieldIndexing::default()
                        .set_index_option(record.into())
                        .set_fieldnorms(fieldnorms)
                        .set_tokenizer(&tokenizer.name());
                    json_options = json_options.set_indexing_options(text_field_indexing);
                }
                if let Some(nested_opts) = nested {
                    json_options = json_options.set_nested();
                    json_options =
                        add_subfields_from_json(json_options, &json!({"nested": nested_opts}));
                }
            }
            SearchFieldConfig::Range { stored, .. } => {
                if stored {
                    json_options = json_options.set_stored();
                }
                // Range must be indexed and fast to be searchable
                let text_field_indexing = TextFieldIndexing::default();
                json_options = json_options.set_indexing_options(text_field_indexing);
                json_options = json_options.set_fast(Some("raw"));
            }
            _ => {
                panic!("attempted to convert non-json search field config to tantivy json config")
            }
        }

        json_options
    }
}

pub fn add_subfields_from_json(
    mut opts: JsonObjectOptions,
    json: &serde_json::Value,
) -> JsonObjectOptions {
    // Work only if the provided JSON value is an object.
    if let serde_json::Value::Object(map) = json {
        // If the object is wrapped in a "nested" key, use that as the mapping.
        let subfields = if let Some(serde_json::Value::Object(nested_map)) = map.get("nested") {
            nested_map
        } else {
            map
        };

        // Process each subfield.
        for (field, sub_val) in subfields {
            // Start with a nested mapping for the field.
            let mut field_opts = JsonObjectOptions::nested();
            // If the JSON for this subfield is an object and not empty,
            // then add subfields recursively.
            if let serde_json::Value::Object(inner_map) = sub_val {
                if !inner_map.is_empty() {
                    field_opts = add_subfields_from_json(field_opts, sub_val);
                }
            }
            // Consume opts and add the new subfield.
            opts = opts.add_subfield(field.clone(), field_opts);
        }
    }
    opts
}

impl From<SearchFieldConfig> for DateOptions {
    fn from(config: SearchFieldConfig) -> Self {
        let mut date_options = DateOptions::default();
        match config {
            SearchFieldConfig::Date {
                indexed,
                fast,
                stored,
                ..
            } => {
                if stored {
                    date_options = date_options.set_stored();
                }
                if fast {
                    date_options = date_options
                        .set_fast()
                        // Match Postgres' maximum allowed precision of microseconds
                        .set_precision(DateTimePrecision::Microseconds);
                }
                if indexed {
                    date_options = date_options.set_indexed();
                }
            }
            _ => {
                panic!("attempted to convert non-date search field config to tantivy date config")
            }
        }
        date_options
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct SearchField {
    /// The id of the field, stored in the index.
    pub id: SearchFieldId,
    /// The name of the field, as it appears to Postgres.
    pub name: SearchFieldName,
    /// Configuration for the field passed at index build time.
    pub config: SearchFieldConfig,
    /// Field type
    pub type_: SearchFieldType,
}

impl From<&SearchField> for Field {
    fn from(val: &SearchField) -> Self {
        val.id.0
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Into)]
pub struct SearchIndexSchema {
    /// The fields that are stored in the index.
    pub fields: Vec<SearchField>,
    /// The index of the key field in the fields vector.
    pub key: usize,
    /// The underlying tantivy schema
    #[into]
    pub schema: Schema,
    /// A lookup cache for retrieving search fields.
    #[serde(skip_serializing)]
    pub lookup: Option<HashMap<SearchFieldName, usize>>,
    /// Whether there are nested JSON fields on this schema.
    pub has_nested: bool,
}

impl SearchIndexSchema {
    pub fn new(
        fields: Vec<(SearchFieldName, SearchFieldConfig, SearchFieldType)>,
        key_index: usize,
    ) -> Result<Self, SearchIndexSchemaError> {
        let mut builder = Schema::builder();
        let mut search_fields = vec![];

        for (name, config, field_type) in fields {
            let id: SearchFieldId = match field_type {
                SearchFieldType::Text => builder.add_text_field(name.as_ref(), config.clone()),
                SearchFieldType::I64 => builder.add_i64_field(name.as_ref(), config.clone()),
                SearchFieldType::U64 => builder.add_u64_field(name.as_ref(), config.clone()),
                SearchFieldType::F64 => builder.add_f64_field(name.as_ref(), config.clone()),
                SearchFieldType::Bool => builder.add_bool_field(name.as_ref(), config.clone()),
                SearchFieldType::Json => {
                    if config.is_nested() {
                        builder.add_nested_json_field(name.as_ref(), config.clone())
                    } else {
                        builder.add_json_field(name.as_ref(), config.clone())
                    }
                }
                SearchFieldType::Range => builder.add_json_field(name.as_ref(), config.clone()),
                SearchFieldType::Date => builder.add_date_field(name.as_ref(), config.clone()),
            }
            .into();

            search_fields.push(SearchField {
                id,
                name,
                config,
                type_: field_type,
            });
        }

        let has_nested = Self::has_nested_static(&search_fields);

        Ok(Self {
            key: key_index,
            schema: builder.build(),
            lookup: Self::build_lookup(&search_fields).into(),
            fields: search_fields,
            has_nested,
        })
    }

    pub fn open(schema: Schema, index_relation: &PgRelation) -> Self {
        let (fields, key_index) = unsafe { get_fields(index_relation) };
        let search_fields = fields
            .iter()
            .map(|(field_name, field_config, field_type)| {
                let field = schema.get_field(field_name.0.as_str()).unwrap();
                SearchField {
                    id: SearchFieldId(field),
                    name: field_name.clone(),
                    config: field_config.clone(),
                    type_: *field_type,
                }
            })
            .collect::<Vec<_>>();

        let has_nested = Self::has_nested_static(&search_fields);
        Self {
            key: key_index,
            schema,
            lookup: Self::build_lookup(&search_fields).into(),
            fields: search_fields,
            has_nested,
        }
    }

    fn build_lookup(search_fields: &[SearchField]) -> HashMap<SearchFieldName, usize> {
        let mut lookup = HashMap::new();
        search_fields
            .iter()
            .enumerate()
            .for_each(|(idx, search_field)| {
                let name = search_field.name.clone();
                lookup.insert(name, idx);
            });
        lookup
    }

    pub fn key_field(&self) -> SearchField {
        self.fields
            .get(self.key)
            .expect("key field should be present on search schema")
            .clone()
    }

    pub fn is_key_field(&self, name: &str) -> bool {
        self.key_field().name.0 == name
    }

    fn has_nested_static(search_fields: &[SearchField]) -> bool {
        let mut has_nested = false;
        for search_field in search_fields {
            if search_field.config.is_nested() {
                has_nested = true;
            }
        }
        has_nested
    }

    pub fn has_nested(&self) -> bool {
        self.schema.has_nested()
    }

    #[inline(always)]
    pub fn new_document(&self) -> SearchDocument {
        SearchDocument {
            doc: tantivy::TantivyDocument::new(),
        }
    }

    pub fn get_search_field(&self, name: &SearchFieldName) -> Option<&SearchField> {
        if let Some(lookup) = &self.lookup {
            lookup.get(name).and_then(|idx| self.fields.get(*idx))
        } else {
            let lookup = Self::build_lookup(&self.fields);
            lookup.get(name).and_then(|idx| self.fields.get(*idx))
        }
    }

    pub fn is_field_raw_sortable(&self, name: &str) -> bool {
        self.is_field_sortable(name, SearchNormalizer::Raw)
            .is_some()
    }

    pub fn is_field_lower_sortable(&self, name: &str) -> bool {
        self.is_field_sortable(name, SearchNormalizer::Lowercase)
            .is_some()
    }

    pub fn is_fast_field(&self, name: &str) -> bool {
        self.is_field_raw_sortable(name)
    }

    pub fn is_numeric_fast_field(&self, name: &str) -> bool {
        if let Some(search_field) = self.get_search_field(&SearchFieldName(name.to_string())) {
            matches!(
                search_field.config,
                SearchFieldConfig::Numeric { fast: true, .. }
                    | SearchFieldConfig::Boolean { fast: true, .. }
                    | SearchFieldConfig::Date { fast: true, .. }
            )
        } else {
            false
        }
    }

    fn is_field_sortable(&self, name: &str, desired_normalizer: SearchNormalizer) -> Option<()> {
        let search_field = self.get_search_field(&SearchFieldName(name.to_string()))?;

        match search_field.config {
            SearchFieldConfig::Text {
                fast: true,
                normalizer,
                ..
            } if normalizer == desired_normalizer => Some(()),
            SearchFieldConfig::Numeric { fast: true, .. } => Some(()),
            SearchFieldConfig::Boolean { fast: true, .. } => Some(()),
            SearchFieldConfig::Date { fast: true, .. } => Some(()),
            _ => None,
        }
    }

    /// A lookup from a Postgres column name to search fields that have
    /// marked it as their source column with the 'column' key.
    pub fn alias_lookup(&self) -> HashMap<String, Vec<&SearchField>> {
        let mut lookup = HashMap::new();
        for field in &self.fields {
            if let Some(column) = field.config.column() {
                lookup
                    .entry(column.to_string())
                    .or_insert_with(Vec::new)
                    .push(field);
            }
        }
        lookup
    }
}

// Index record schema
#[allow(unused)] // used by serde
pub enum IndexRecordOptionSchema {
    Basic,
    WithFreqs,
    WithFreqsAndPositions,
}

#[derive(Debug, Serialize, Deserialize, Clone, Ord, PartialOrd, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct IndexRecordOption(tantivy::schema::IndexRecordOption);

#[allow(non_upper_case_globals)]
impl IndexRecordOption {
    pub const Basic: IndexRecordOption =
        IndexRecordOption(tantivy::schema::IndexRecordOption::Basic);
    pub const WithFreqs: IndexRecordOption =
        IndexRecordOption(tantivy::schema::IndexRecordOption::WithFreqs);
    pub const WithFreqsAndPositions: IndexRecordOption =
        IndexRecordOption(tantivy::schema::IndexRecordOption::WithFreqsAndPositions);
}

impl From<tantivy::schema::IndexRecordOption> for IndexRecordOption {
    #[inline]
    fn from(value: tantivy::schema::IndexRecordOption) -> Self {
        Self(value)
    }
}

impl From<IndexRecordOption> for tantivy::schema::IndexRecordOption {
    fn from(value: IndexRecordOption) -> Self {
        value.0
    }
}

impl Display for IndexRecordOption {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            tantivy::schema::IndexRecordOption::Basic => write!(f, "basic"),
            tantivy::schema::IndexRecordOption::WithFreqs => write!(f, "freq"),
            tantivy::schema::IndexRecordOption::WithFreqsAndPositions => write!(f, "position"),
        }
    }
}

#[derive(Debug, Error)]
pub enum SearchIndexSchemaError {
    #[error("invalid field type for numeric: {0:?}")]
    InvalidNumericType(SearchFieldType),
    #[error("invalid postgres oid passed to search index schema: {0:?}")]
    InvalidPgOid(PgOid),
    #[error("no key field specified for search index")]
    NoKeyFieldSpecified,
    #[error("no ctid field specified for search index")]
    NoCtidFieldSpecified,
}

fn default_as_true() -> bool {
    true
}

fn default_as_false() -> bool {
    true
}

fn default_as_freqs_and_positions() -> IndexRecordOption {
    IndexRecordOption(tantivy::schema::IndexRecordOption::WithFreqsAndPositions)
}

trait AsTypeOid {
    fn typeoid(&self, field: &SearchField) -> PgOid;
}

impl AsTypeOid for (&PgRelation, &SearchIndexSchema) {
    fn typeoid(&self, search_field: &SearchField) -> PgOid {
        if search_field.name.0 == "ctid" {
            return PgOid::BuiltIn(pgrx::pg_sys::BuiltinOid::TIDOID);
        }
        let indexrel = self.0;
        for attribute in indexrel.tuple_desc().iter() {
            let attname = attribute.name().to_string();
            let typeoid = attribute.type_oid();
            if search_field.name.0 == attname {
                return typeoid;
            }
            // If the field was aliased, return the column
            // it points to.
            if search_field.config.column() == Some(&attname) {
                return typeoid;
            }
        }
        panic!(
            "search field {} not found in index '{}' with oid: {}",
            search_field.name.0,
            indexrel.name(),
            indexrel.oid().as_u32()
        );
    }
}

impl AsTypeOid for HashMap<String, PgOid> {
    fn typeoid(&self, search_field: &SearchField) -> PgOid {
        if search_field.name.0 == "ctid" {
            return PgOid::BuiltIn(pgrx::pg_sys::BuiltinOid::TIDOID);
        }
        self.get(&search_field.name.0)
            .copied()
            .unwrap_or_else(|| panic!("search field {} not found in index", search_field.name.0))
    }
}

impl AsFieldType<String> for (&PgRelation, &SearchIndexSchema) {
    fn key_field(&self) -> (tantivy::schema::FieldType, PgOid, Field) {
        let search_field = self.1.key_field();
        let field = search_field.id.0;
        let field_type = self.1.schema.get_field_entry(field).field_type().clone();
        (field_type, self.typeoid(&search_field), field)
    }

    fn fields(&self) -> Vec<(tantivy::schema::FieldType, PgOid, Field)> {
        let indexrel = self.0;
        let typeoid_lookup: HashMap<String, PgOid> = indexrel
            .tuple_desc()
            .iter()
            .map(|attribute| (attribute.name().to_string(), attribute.type_oid()))
            .collect();
        self.1
            .fields
            .iter()
            .map(|search_field| {
                let field = search_field.id.0;
                let field_type = self.1.schema.get_field_entry(field).field_type().clone();
                (field_type, typeoid_lookup.typeoid(search_field), field)
            })
            .collect()
    }
    fn as_field_type(&self, from: &String) -> Option<(tantivy::schema::FieldType, PgOid, Field)> {
        self.1
            .get_search_field(&SearchFieldName(from.into()))
            .map(|search_field| {
                let field = search_field.id.0;
                let field_type = self.1.schema.get_field_entry(field).field_type().clone();
                (field_type, self.typeoid(search_field), field)
            })
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use tantivy::schema::{JsonObjectOptions, NumericOptions, TextOptions};

    use crate::schema::SearchFieldConfig;

    #[rstest]
    fn test_search_text_options() {
        let json = r#"{
            "indexed": true,
            "fast": false,
            "stored": true,
            "fieldnorms": true,
            "type": "default",
            "record": "basic",
            "normalizer": "raw"
        }"#;
        let config: serde_json::Value = serde_json::from_str(json).unwrap();
        let search_text_option: SearchFieldConfig =
            serde_json::from_value(serde_json::json!({"Text": config})).unwrap();
        let expected: TextOptions = search_text_option.into();

        let text_options: TextOptions = SearchFieldConfig::default_text().into();
        assert_eq!(expected.is_stored(), text_options.is_stored());
        assert_eq!(
            expected.get_fast_field_tokenizer_name(),
            text_options.get_fast_field_tokenizer_name()
        );

        let text_options = text_options.set_fast(Some("index"));
        assert_ne!(expected.is_fast(), text_options.is_fast());
    }

    #[rstest]
    fn test_search_numeric_options() {
        let json = r#"{
            "indexed": true,
            "stored": true,
            "fieldnorms": false,
            "fast": true
        }"#;
        let config: serde_json::Value = serde_json::from_str(json).unwrap();
        let expected: SearchFieldConfig =
            serde_json::from_value(serde_json::json!({"Numeric": config})).unwrap();
        let int_options: NumericOptions = SearchFieldConfig::default_numeric().into();

        assert_eq!(int_options, expected.into());
    }

    #[rstest]
    fn test_search_boolean_options() {
        let json = r#"{
            "indexed": true,
            "stored": true,
            "fieldnorms": false,
            "fast": true
        }"#;
        let config: serde_json::Value = serde_json::from_str(json).unwrap();
        let expected: SearchFieldConfig =
            serde_json::from_value(serde_json::json!({"Boolean": config})).unwrap();
        let int_options: NumericOptions = SearchFieldConfig::default_numeric().into();

        assert_eq!(int_options, expected.into());
    }

    #[rstest]
    fn test_search_jsonobject_options() {
        let json = r#"{
            "indexed": true,
            "fast": false,
            "stored": true,
            "expand_dots": true,
            "type": "default",
            "record": "basic",
            "normalizer": "raw"
        }"#;
        let config: serde_json::Value = serde_json::from_str(json).unwrap();
        let search_json_option: SearchFieldConfig =
            serde_json::from_value(serde_json::json!({"Json": config})).unwrap();
        let expected: JsonObjectOptions = search_json_option.into();

        let json_object_options: JsonObjectOptions = SearchFieldConfig::default_json().into();
        assert_eq!(expected.is_stored(), json_object_options.is_stored());
        assert_eq!(
            expected.get_fast_field_tokenizer_name(),
            json_object_options.get_fast_field_tokenizer_name()
        );
        assert_eq!(
            expected.is_expand_dots_enabled(),
            json_object_options.is_expand_dots_enabled()
        );

        let text_options = json_object_options.set_fast(Some("index"));
        assert_ne!(expected.is_fast(), text_options.is_fast());
    }
}
