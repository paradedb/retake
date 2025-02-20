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

use crate::index::merge_policy::MergeLock;
use crate::index::reader::index::SearchIndexReader;
use crate::index::writer::index::SearchIndexWriter;
use crate::index::BlockDirectoryType;
use crate::postgres::storage::block::{
    MergeLockData, SegmentMetaEntry, CLEANUP_LOCK, MERGE_LOCK, SCHEMA_START, SEGMENT_METAS_START,
    SETTINGS_START,
};
use crate::postgres::storage::buffer::BufferManager;
use crate::postgres::storage::{LinkedBytesList, LinkedItemList};
use crate::postgres::utils::{
    categorize_fields, item_pointer_to_u64, row_to_search_document, CategorizedFieldData,
};
use crate::schema::SearchField;
use pgrx::*;
use std::ffi::CStr;
use std::time::Instant;

// For now just pass the count on the build callback state
struct BuildState {
    count: usize,
    per_row_context: PgMemoryContexts,
    start: Instant,
    writer: SearchIndexWriter,
    categorized_fields: Vec<(SearchField, CategorizedFieldData)>,
    key_field_name: String,
}

impl BuildState {
    fn new(indexrel: &PgRelation, writer: SearchIndexWriter) -> Self {
        let tupdesc = unsafe { PgTupleDesc::from_pg_unchecked(indexrel.rd_att) };
        let categorized_fields = categorize_fields(&tupdesc, &writer.schema);
        let key_field_name = writer.schema.key_field().name.0;

        BuildState {
            count: 0,
            per_row_context: PgMemoryContexts::new("pg_search ambuild context"),
            start: Instant::now(),
            writer,
            categorized_fields,
            key_field_name,
        }
    }
}

#[pg_guard]
pub extern "C" fn ambuild(
    heaprel: pg_sys::Relation,
    indexrel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let heap_relation = unsafe { PgRelation::from_pg(heaprel) };
    let index_relation = unsafe { PgRelation::from_pg(indexrel) };
    let index_oid = index_relation.oid();

    // Create the metadata blocks for the index
    unsafe { create_metadata(&index_relation) };

    // ensure we only allow one `USING bm25` index on this relation, accounting for a REINDEX
    // and accounting for CONCURRENTLY.
    unsafe {
        let index_tuple = &(*index_relation.rd_index);
        let is_reindex = !index_tuple.indisvalid;
        let is_concurrent = (*index_info).ii_Concurrent;

        if !is_reindex {
            for existing_index in heap_relation.indices(pg_sys::AccessShareLock as _) {
                if existing_index.oid() == index_oid {
                    // the index we're about to build already exists on the table.
                    continue;
                }

                if is_bm25_index(&existing_index) && !is_concurrent {
                    panic!("a relation may only have one `USING bm25` index");
                }
            }
        }
    }

    let tuple_count = do_heap_scan(index_info, &heap_relation, &index_relation);
    unsafe { pg_sys::FlushRelationBuffers(indexrel) };

    let mut result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    result.heap_tuples = tuple_count as f64;
    result.index_tuples = tuple_count as f64;
    result.into_pg()
}

#[pg_guard]
pub extern "C" fn ambuildempty(_index_relation: pg_sys::Relation) {}

fn do_heap_scan<'a>(
    index_info: *mut pg_sys::IndexInfo,
    heap_relation: &'a PgRelation,
    index_relation: &'a PgRelation,
) -> usize {
    unsafe {
        let writer = SearchIndexWriter::create_index(index_relation)
            .expect("do_heap_scan: should be able to open a SearchIndexWriter");
        let mut state = BuildState::new(index_relation, writer);

        pg_sys::IndexBuildHeapScan(
            heap_relation.as_ptr(),
            index_relation.as_ptr(),
            index_info,
            Some(build_callback),
            &mut state,
        );

        state
            .writer
            .commit()
            .unwrap_or_else(|e| panic!("failed to commit new tantivy index: {e}"));

        // store number of segments created in metadata
        SearchIndexReader::open(index_relation, BlockDirectoryType::Mvcc, false)
            .expect("do_heap_scan: should be able to open a SearchIndexReader");
        MergeLock::init(index_relation.oid());

        state.count
    }
}

#[pg_guard]
unsafe extern "C" fn build_callback(
    indexrel: pg_sys::Relation,
    ctid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    check_for_interrupts!();
    let build_state = (state as *mut BuildState)
        .as_mut()
        .expect("BuildState pointer should not be null");

    let categorized_fields = &build_state.categorized_fields;
    let key_field_name = &build_state.key_field_name;
    let writer = &mut build_state.writer;
    // In the block below, we switch to the memory context we've defined on our build
    // state, resetting it before and after. We do this because we're looking up a
    // PgTupleDesc... which is supposed to free the corresponding Postgres memory when it
    // is dropped. However, in practice, we're not seeing the memory get freed, which is
    // causing huge memory usage when building large indexes.
    //
    // By running in our own memory context, we can force the memory to be freed with
    // the call to reset().
    unsafe {
        build_state.per_row_context.switch_to(|cxt| {
            let mut search_document = writer.schema.new_document();

            row_to_search_document(
                values,
                isnull,
                key_field_name,
                categorized_fields,
                &mut search_document,
            )
                .unwrap_or_else(|err| {
                    panic!(
                        "error creating index entries for index '{}': {err}",
                        CStr::from_ptr((*(*indexrel).rd_rel).relname.data.as_ptr()).to_string_lossy()
                    );
                });
            writer
                .insert(search_document, item_pointer_to_u64(*ctid))
                .unwrap_or_else(|err| {
                    panic!("error inserting document during build callback.  See Postgres log for more information: {err:?}")
                });

            cxt.reset();
        });

        // important to count the number of items we've indexed for proper statistics updates,
        // especially after CREATE INDEX has finished
        build_state.count += 1;

        if crate::gucs::log_create_index_progress() && build_state.count % 100_000 == 0 {
            let secs = build_state.start.elapsed().as_secs_f64();
            let rate = build_state.count as f64 / secs;
            pgrx::log!(
                "processed {} rows in {secs:.2} seconds ({rate:.2} per second)",
                build_state.count,
            );
        }
    }
}

fn is_bm25_index(indexrel: &PgRelation) -> bool {
    unsafe {
        // SAFETY:  we ensure that `indexrel.rd_indam` is non null and can be dereferenced
        !indexrel.rd_indam.is_null() && (*indexrel.rd_indam).ambuild == Some(ambuild)
    }
}

unsafe fn create_metadata(index_relation: &PgRelation) {
    let relation_oid = index_relation.oid();
    let mut bman = BufferManager::new(relation_oid);

    // Init merge lock buffer
    let mut merge_lock = bman.new_buffer();
    assert_eq!(merge_lock.number(), MERGE_LOCK);
    let mut page = merge_lock.init_page();
    let metadata = page.contents_mut::<MergeLockData>();
    metadata.last_merge = pg_sys::InvalidTransactionId;

    // Init cleanup lock buffer
    let mut cleanup_lock = bman.new_buffer();
    assert_eq!(cleanup_lock.number(), CLEANUP_LOCK);
    cleanup_lock.init_page();

    // initialize all the other required buffers
    let schema = LinkedBytesList::create(relation_oid);
    let settings = LinkedBytesList::create(relation_oid);
    let segment_metas = LinkedItemList::<SegmentMetaEntry>::create(relation_oid);

    assert_eq!(schema.header_blockno, SCHEMA_START);
    assert_eq!(settings.header_blockno, SETTINGS_START);
    assert_eq!(segment_metas.header_blockno, SEGMENT_METAS_START);
}
