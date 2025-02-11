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

use std::collections::HashMap;

use crate::index::reader::index::SearchIndexReader;
use crate::index::BlockDirectoryType;
use crate::postgres::options::SearchIndexCreateOptions;
use crate::postgres::storage::block::{
    LinkedList, MVCCEntry, SegmentMetaEntry, SEGMENT_METAS_START,
};
use crate::postgres::storage::LinkedItemList;
use crate::postgres::utils::item_pointer_to_u64;
use crate::query::SearchQueryInput;
use crate::schema::IndexRecordOption;
use crate::schema::SearchFieldConfig;
use crate::schema::SearchFieldName;
use crate::schema::SearchFieldType;
use anyhow::bail;
use anyhow::Result;
use pgrx::prelude::*;
use pgrx::JsonB;
use pgrx::PgRelation;
use serde_json::Map;
use serde_json::Value;
use tokenizers::manager::SearchTokenizerFilters;
use tokenizers::SearchNormalizer;
use tokenizers::SearchTokenizer;

#[allow(clippy::too_many_arguments)]
#[pg_extern]
fn format_create_bm25(
    index_name: &str,
    table_name: &str,
    key_field: &str,
    schema_name: default!(&str, "''"),
    text_fields: default!(JsonB, "'{}'::jsonb"),
    numeric_fields: default!(JsonB, "'{}'::jsonb"),
    boolean_fields: default!(JsonB, "'{}'::jsonb"),
    json_fields: default!(JsonB, "'{}'::jsonb"),
    range_fields: default!(JsonB, "'{}'::jsonb"),
    datetime_fields: default!(JsonB, "'{}'::jsonb"),
    predicates: default!(&str, "''"),
) -> Result<String> {
    let mut column_names = vec![key_field.to_string()];
    for fields in [
        &text_fields,
        &numeric_fields,
        &boolean_fields,
        &json_fields,
        &range_fields,
        &datetime_fields,
    ] {
        if let Value::Object(ref map) = fields.0 {
            for key in map.keys() {
                if key != key_field {
                    column_names.push(spi::quote_identifier(key.clone()));
                }
            }
        } else {
            bail!("Expected a JSON object, received: {}", fields.0);
        }
    }

    let column_names_csv = column_names
        .clone()
        .into_iter()
        .filter(|name| name != key_field)
        .collect::<Vec<String>>()
        .join(", ");

    let predicate_where = if !predicates.is_empty() {
        format!("WHERE {}", predicates)
    } else {
        "".to_string()
    };

    let schema_prefix = if schema_name.is_empty() {
        "".to_string()
    } else {
        format!("{}.", spi::quote_identifier(schema_name))
    };

    Ok(format!(
        "CREATE INDEX {} ON {}{} USING bm25 ({}, {}) WITH (key_field={}, text_fields={}, numeric_fields={}, boolean_fields={}, json_fields={}, range_fields={}, datetime_fields={}) {};",
        spi::quote_identifier(index_name),
        schema_prefix,
        spi::quote_identifier(table_name),
        spi::quote_identifier(key_field),
        column_names_csv,
        spi::quote_literal(key_field),
        spi::quote_literal(&serde_json::to_string(&text_fields)?),
        spi::quote_literal(&serde_json::to_string(&numeric_fields)?),
        spi::quote_literal(&serde_json::to_string(&boolean_fields)?),
        spi::quote_literal(&serde_json::to_string(&json_fields)?),
        spi::quote_literal(&serde_json::to_string(&range_fields)?),
        spi::quote_literal(&serde_json::to_string(&datetime_fields)?),
        predicate_where))
}

#[pg_extern]
pub unsafe fn index_fields(index: PgRelation) -> JsonB {
    // # Safety
    //
    // Lock the index relation until the end of this function so it is not dropped or
    // altered while we are reading it.
    //
    // Because we accept a PgRelation above, we have confidence that Postgres has already
    // validated the existence of the relation. We are safe calling the function below as
    // long we do not pass pg_sys::NoLock without any other locking mechanism of our own.
    let index = unsafe { PgRelation::with_lock(index.oid(), pg_sys::AccessShareLock as _) };
    let rdopts: PgBox<SearchIndexCreateOptions> = if !index.rd_options.is_null() {
        unsafe { PgBox::from_pg(index.rd_options as *mut SearchIndexCreateOptions) }
    } else {
        let ops = unsafe { PgBox::<SearchIndexCreateOptions>::alloc0() };
        ops.into_pg_boxed()
    };

    // Create a map from column name to column type. We'll use this to verify that index
    // configurations passed by the user reference the correct types for each column.
    let name_type_map: HashMap<SearchFieldName, SearchFieldType> = index
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

    // Parse and validate the index configurations for each column.
    let text_fields =
        rdopts
            .get_text_fields()
            .into_iter()
            .map(|(name, config)| match name_type_map.get(&name) {
                Some(field_type @ SearchFieldType::Text) => (name, config, *field_type),
                _ => panic!("'{name}' cannot be indexed as a text field"),
            });

    let numeric_fields = rdopts
        .get_numeric_fields()
        .into_iter()
        .map(|(name, config)| match name_type_map.get(&name) {
            Some(field_type @ SearchFieldType::U64)
            | Some(field_type @ SearchFieldType::I64)
            | Some(field_type @ SearchFieldType::F64) => (name, config, *field_type),
            _ => panic!("'{name}' cannot be indexed as a numeric field"),
        });

    let boolean_fields = rdopts
        .get_boolean_fields()
        .into_iter()
        .map(|(name, config)| match name_type_map.get(&name) {
            Some(field_type @ SearchFieldType::Bool) => (name, config, *field_type),
            _ => panic!("'{name}' cannot be indexed as a boolean field"),
        });

    let json_fields =
        rdopts
            .get_json_fields()
            .into_iter()
            .map(|(name, config)| match name_type_map.get(&name) {
                Some(field_type @ SearchFieldType::Json) => (name, config, *field_type),
                _ => panic!("'{name}' cannot be indexed as a JSON field"),
            });

    let range_fields = rdopts.get_range_fields().into_iter().map(|(name, config)| {
        match name_type_map.get(&name) {
            Some(field_type @ SearchFieldType::Range) => (name, config, *field_type),
            _ => panic!("'{name}' cannot be indexed as a range field"),
        }
    });

    let datetime_fields = rdopts
        .get_datetime_fields()
        .into_iter()
        .map(|(name, config)| match name_type_map.get(&name) {
            Some(field_type @ SearchFieldType::Date) => (name, config, *field_type),
            _ => panic!("'{name}' cannot be indexed as a datetime field"),
        });

    let key_field = rdopts.get_key_field().expect("must specify key_field");
    let key_field_type = match name_type_map.get(&key_field) {
        Some(field_type) => field_type,
        None => panic!("key field does not exist"),
    };
    let key_config = match key_field_type {
        SearchFieldType::I64 | SearchFieldType::U64 | SearchFieldType::F64 => {
            SearchFieldConfig::Numeric {
                indexed: true,
                fast: true,
                stored: false,
                column: None,
            }
        }
        SearchFieldType::Text => SearchFieldConfig::Text {
            indexed: true,
            fast: true,
            stored: false,
            fieldnorms: false,
            tokenizer: SearchTokenizer::Raw(SearchTokenizerFilters::default()),
            record: IndexRecordOption::Basic,
            normalizer: SearchNormalizer::Raw,
            column: None,
        },
        SearchFieldType::Json => SearchFieldConfig::Json {
            indexed: true,
            fast: true,
            stored: false,
            expand_dots: false,
            tokenizer: SearchTokenizer::Raw(SearchTokenizerFilters::default()),
            record: IndexRecordOption::Basic,
            normalizer: SearchNormalizer::Raw,
            fieldnorms: true,
            column: None,
            nested: None,
        },
        SearchFieldType::Range => SearchFieldConfig::Range {
            stored: false,
            column: None,
        },
        SearchFieldType::Bool => SearchFieldConfig::Boolean {
            indexed: true,
            fast: true,
            stored: false,
            column: None,
        },
        SearchFieldType::Date => SearchFieldConfig::Date {
            indexed: true,
            fast: true,
            stored: false,
            column: None,
        },
    };

    // Concatenate the separate lists of fields.
    let fields = text_fields
        .chain(numeric_fields)
        .chain(boolean_fields)
        .chain(json_fields)
        .chain(range_fields)
        .chain(datetime_fields)
        .chain(std::iter::once((
            key_field.clone(),
            key_config,
            *key_field_type,
        )))
        .map(|(name, config, _)| {
            (
                name.0,
                serde_json::to_value(config)
                    .expect("must be able to convert search field config to JSON"),
            )
        })
        .collect::<Map<_, _>>();

    JsonB(serde_json::Value::from(fields))
}

#[allow(clippy::type_complexity)]
#[pg_extern]
fn index_info(
    index: PgRelation,
    show_invisible: default!(bool, false),
) -> anyhow::Result<
    TableIterator<
        'static,
        (
            name!(visible, bool),
            name!(recyclable, bool),
            name!(xmin, AnyNumeric),
            name!(xmax, AnyNumeric),
            name!(segno, String),
            name!(byte_size, Option<AnyNumeric>),
            name!(num_docs, Option<AnyNumeric>),
            name!(num_deleted, Option<AnyNumeric>),
            name!(termdict_bytes, Option<AnyNumeric>),
            name!(postings_bytes, Option<AnyNumeric>),
            name!(positions_bytes, Option<AnyNumeric>),
            name!(fast_fields_bytes, Option<AnyNumeric>),
            name!(fieldnorms_bytes, Option<AnyNumeric>),
            name!(store_bytes, Option<AnyNumeric>),
            name!(deletes_bytes, Option<AnyNumeric>),
        ),
    >,
> {
    // # Safety
    //
    // Lock the index relation until the end of this function so it is not dropped or
    // altered while we are reading it.
    //
    // Because we accept a PgRelation above, we have confidence that Postgres has already
    // validated the existence of the relation. We are safe calling the function below as
    // long we do not pass pg_sys::NoLock without any other locking mechanism of our own.
    let index = unsafe { PgRelation::with_lock(index.oid(), pg_sys::AccessShareLock as _) };
    let heap = index
        .heap_relation()
        .expect("index must have a heap relation");

    // open the specified index
    let search_index = SearchIndexReader::open(&index, BlockDirectoryType::Mvcc, false)?;
    let mut search_readers = search_index
        .segment_readers()
        .iter()
        .map(|segment_reader| (segment_reader.segment_id(), segment_reader))
        .collect::<HashMap<_, _>>();
    let all_entries = unsafe {
        LinkedItemList::<SegmentMetaEntry>::open(index.oid(), SEGMENT_METAS_START)
            .list()
            .into_iter()
            .map(|entry| (entry.segment_id, entry))
            .collect::<HashMap<_, _>>()
    };

    let snapshot = unsafe { pg_sys::GetActiveSnapshot() };
    let mut results = Vec::new();
    for (segment_id, entry) in all_entries {
        if !show_invisible && unsafe { !entry.visible(snapshot) } {
            continue;
        }
        let segment_reader = search_readers.remove(&segment_id);
        let space_usage = segment_reader.map(|reader| {
            reader
                .space_usage()
                .expect("should be able to get space usage")
        });
        let space_usage = space_usage.as_ref();
        results.push((
            unsafe { entry.visible(snapshot) },
            unsafe { entry.recyclable(snapshot, heap.as_ptr()) },
            entry.xmin.into(),
            entry.xmax.into(),
            segment_id.short_uuid_string(),
            space_usage
                .map(|usage| usage.total().get_bytes().into())
                .or_else(|| Some(entry.byte_size().into())),
            segment_reader
                .map(|reader| reader.num_docs().into())
                .or_else(|| Some(entry.num_docs().into())),
            segment_reader
                .map(|reader| reader.num_deleted_docs().into())
                .or_else(|| Some(entry.num_deleted_docs().into())),
            space_usage
                .map(|usage| usage.termdict().total().get_bytes().into())
                .or_else(|| entry.terms.map(|file| file.total_bytes.into())),
            space_usage
                .map(|usage| usage.postings().total().get_bytes().into())
                .or_else(|| entry.postings.map(|file| file.total_bytes.into())),
            space_usage
                .map(|usage| usage.positions().total().get_bytes().into())
                .or_else(|| entry.positions.map(|file| file.total_bytes.into())),
            space_usage
                .map(|usage| usage.fast_fields().total().get_bytes().into())
                .or_else(|| entry.fast_fields.map(|file| file.total_bytes.into())),
            space_usage
                .map(|usage| usage.fieldnorms().total().get_bytes().into())
                .or_else(|| entry.field_norms.map(|file| file.total_bytes.into())),
            space_usage
                .map(|usage| usage.store().total().get_bytes().into())
                .or_else(|| entry.store.map(|file| file.total_bytes.into())),
            space_usage
                .map(|usage| usage.deletes().get_bytes().into())
                .or_else(|| entry.delete.map(|file| file.file_entry.total_bytes.into())),
        ));
    }

    assert!(search_readers.is_empty());

    Ok(TableIterator::new(results))
}

/// Returns the list of segments that contain the specified [`pg_sys::ItemPointerData]` heap tuple
/// identifier.
///
/// If the specified `ctid` is the result of a HOT chain update, then it's likely this function will
/// return NULL -- HOT chains cannot be "reverse searched".
///
/// Otherwise, if this function returns NULL that is likely indicative of a pg_search bug.  We wish
/// you luck in determining which is which.
#[pg_extern]
fn find_ctid(index: PgRelation, ctid: pg_sys::ItemPointerData) -> Result<Option<Vec<String>>> {
    let index = unsafe { PgRelation::with_lock(index.oid(), pg_sys::AccessShareLock as _) };

    let search_index = SearchIndexReader::open(&index, BlockDirectoryType::Mvcc, true)?;
    let ctid_u64 = item_pointer_to_u64(ctid);
    let results = search_index.search(
        false,
        false,
        &SearchQueryInput::Term {
            field: Some("ctid".into()),
            value: ctid_u64.into(),
            is_datetime: false,
        },
        None,
    );

    let results = results
        .map(|(_, doc_address)| {
            search_index.segment_readers()[doc_address.segment_ord as usize]
                .segment_id()
                .short_uuid_string()
        })
        .collect::<Vec<_>>();

    if results.is_empty() {
        pgrx::warning!(
            "find_ctid: didn't find segment for: {:?}.  segments={:#?}",
            pgrx::itemptr::item_pointer_get_both(ctid),
            search_index.segment_ids()
        );
        Ok(None)
    } else {
        Ok(Some(results))
    }
}

#[pg_extern]
fn validate_checksum(index: PgRelation) -> Result<SetOfIterator<'static, String>> {
    // # Safety
    //
    // Lock the index relation until the end of this function so it is not dropped or
    // altered while we are reading it.
    //
    // Because we accept a PgRelation above, we have confidence that Postgres has already
    // validated the existence of the relation. We are safe calling the function below as
    // long we do not pass pg_sys::NoLock without any other locking mechanism of our own.
    let index = unsafe { PgRelation::with_lock(index.oid(), pg_sys::AccessShareLock as _) };

    // open the specified index
    let search_reader = SearchIndexReader::open(&index, BlockDirectoryType::Mvcc, false)?;

    let failed = search_reader.validate_checksum()?;
    Ok(SetOfIterator::new(
        failed.into_iter().map(|path| path.display().to_string()),
    ))
}

#[pg_extern(sql = "")]
fn create_bm25_jsonb() {}

#[allow(clippy::type_complexity)]
#[pg_extern]
fn storage_info(
    index: PgRelation,
) -> TableIterator<'static, (name!(block, i64), name!(max_offset, i32))> {
    let segment_components =
        LinkedItemList::<SegmentMetaEntry>::open(index.oid(), SEGMENT_METAS_START);
    let bman = segment_components.bman();
    let mut blockno = segment_components.get_start_blockno();
    let mut data = vec![];

    while blockno != pg_sys::InvalidBlockNumber {
        let buffer = bman.get_buffer(blockno);
        let page = buffer.page();
        let max_offset = page.max_offset_number();
        data.push((blockno as i64, max_offset as i32));
        blockno = page.next_blockno();
    }

    TableIterator::new(data)
}

#[allow(clippy::type_complexity)]
#[pg_extern]
fn page_info(
    index: PgRelation,
    blockno: i64,
) -> anyhow::Result<
    TableIterator<
        'static,
        (
            name!(offsetno, i32),
            name!(size, i32),
            name!(visible, bool),
            name!(recyclable, bool),
            name!(contents, JsonB),
        ),
    >,
> {
    let segment_components =
        LinkedItemList::<SegmentMetaEntry>::open(index.oid(), SEGMENT_METAS_START);
    let bman = segment_components.bman();
    let buffer = bman.get_buffer(blockno as pg_sys::BlockNumber);
    let page = buffer.page();
    let max_offset = page.max_offset_number();

    if max_offset == pg_sys::InvalidOffsetNumber {
        return Ok(TableIterator::new(vec![]));
    }

    let mut data = vec![];
    let snapshot = unsafe { pg_sys::GetActiveSnapshot() };
    let heap_oid = unsafe { pg_sys::IndexGetRelation(index.oid(), false) };
    let heap_relation = unsafe { pg_sys::RelationIdGetRelation(heap_oid) };

    for offsetno in pg_sys::FirstOffsetNumber..=max_offset {
        unsafe {
            if let Some((entry, size)) = page.read_item::<SegmentMetaEntry>(offsetno) {
                data.push((
                    offsetno as i32,
                    size as i32,
                    entry.visible(snapshot),
                    entry.recyclable(snapshot, heap_relation),
                    JsonB(serde_json::to_value(entry)?),
                ))
            } else {
                data.push((offsetno as i32, 0_i32, false, false, JsonB(Value::Null)))
            }
        }
    }

    unsafe { pg_sys::RelationClose(heap_relation) };
    Ok(TableIterator::new(data))
}

#[pg_extern]
fn version_info() -> TableIterator<
    'static,
    (
        name!(version, String),
        name!(githash, String),
        name!(build_mode, String),
    ),
> {
    let version = option_env!("CARGO_PKG_VERSION")
        .unwrap_or("unknown")
        .to_string();

    let git_sha = option_env!("VERGEN_GIT_SHA")
        .unwrap_or("unknown")
        .to_string();

    let build_mode = if cfg!(debug_assertions) {
        "debug".to_string()
    } else {
        "release".to_string()
    };

    TableIterator::once((version, git_sha, build_mode))
}
