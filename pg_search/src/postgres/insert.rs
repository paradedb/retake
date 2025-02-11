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

use crate::index::writer::index::SearchIndexWriter;
use crate::index::{BlockDirectoryType, WriterResources};
use crate::postgres::utils::{
    categorize_fields, item_pointer_to_u64, row_to_search_document, CategorizedFieldData,
};
use crate::schema::SearchField;
use pgrx::{pg_guard, pg_sys, PgRelation, PgTupleDesc};
use std::ffi::CStr;
use std::panic::{catch_unwind, resume_unwind};

use super::utils::row_to_search_documents;

extern "C" {
    fn IsLogicalWorker() -> bool;
}

pub struct InsertState {
    pub writer: Option<SearchIndexWriter>,
    categorized_fields: Vec<(SearchField, CategorizedFieldData)>,
    key_field_name: String,
}

impl InsertState {
    unsafe fn new(
        indexrel: &PgRelation,
        writer_resources: WriterResources,
    ) -> anyhow::Result<Self> {
        let writer = SearchIndexWriter::open(indexrel, BlockDirectoryType::Mvcc, writer_resources)?;
        let tupdesc = unsafe { PgTupleDesc::from_pg_unchecked(indexrel.rd_att) };
        let categorized_fields = categorize_fields(&tupdesc, &writer.schema);
        let key_field_name = writer.schema.key_field().name.0;
        Ok(Self {
            writer: Some(writer),
            categorized_fields,
            key_field_name,
        })
    }
}

#[cfg(not(feature = "pg17"))]
unsafe fn init_insert_state(
    index_relation: pg_sys::Relation,
    index_info: &mut pg_sys::IndexInfo,
    writer_resources: WriterResources,
) -> &'static mut InsertState {
    use crate::postgres::fake_aminsertcleanup::{
        get_pending_insert_state, reset_pending_insert_state, set_pending_insert_state,
    };

    if index_info.ii_AmCache.is_null() {
        let index_relation = PgRelation::from_pg(index_relation);
        let state = InsertState::new(&index_relation, writer_resources)
            .expect("should be able to open new SearchIndex for writing");

        set_pending_insert_state(state);
        index_info.ii_AmCache = &true as *const _ as *mut _; // a pointer to `true` to indicate that we've set up the InsertState
        pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Abort, || {
            reset_pending_insert_state()
        });
    }

    get_pending_insert_state().expect("should have a pending insert state")
}

#[cfg(feature = "pg17")]
pub unsafe fn init_insert_state(
    index_relation: pg_sys::Relation,
    index_info: &mut pg_sys::IndexInfo,
    writer_resources: WriterResources,
) -> &mut InsertState {
    if index_info.ii_AmCache.is_null() {
        // we don't have any cached state yet, so create it now
        let index_relation = PgRelation::from_pg(index_relation);
        let state = InsertState::new(&index_relation, writer_resources)
            .expect("should be able to open new SearchIndex for writing");

        // leak it into the MemoryContext for this scan (as specified by the IndexInfo argument)
        //
        // When that memory context is freed by Postgres is when we'll do our tantivy commit/abort
        // of the changes made during `aminsert`
        //
        // SAFETY: `leak_and_drop_on_delete` palloc's memory in CurrentMemoryContext, but in this
        // case we want the thing it allocates to be palloc'd in the `ii_Context`
        pgrx::PgMemoryContexts::For(index_info.ii_Context)
            .switch_to(|mcxt| index_info.ii_AmCache = mcxt.leak_and_drop_on_delete(state).cast())
    };

    &mut *index_info.ii_AmCache.cast()
}

#[allow(clippy::too_many_arguments)]
#[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
#[pg_guard]
pub unsafe extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck::Type,
    _index_unchanged: bool,
    index_info: *mut pg_sys::IndexInfo,
) -> bool {
    aminsert_internal(index_relation, values, isnull, heap_tid, index_info)
}

#[cfg(feature = "pg13")]
#[pg_guard]
pub unsafe extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck::Type,
    index_info: *mut pg_sys::IndexInfo,
) -> bool {
    aminsert_internal(index_relation, values, isnull, heap_tid, index_info)
}

#[inline(always)]
unsafe fn aminsert_internal(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    ctid: pg_sys::ItemPointer,
    index_info: *mut pg_sys::IndexInfo,
) -> bool {
    if IsLogicalWorker() {
        panic!("pg_search logical replication is an enterprise feature");
    }

    let result = catch_unwind(|| {
        let state = init_insert_state(
            index_relation,
            index_info
                .as_mut()
                .expect("index_info argument must not be null"),
            WriterResources::Statement,
        );
        let categorized_fields = &state.categorized_fields;
        let key_field_name = &state.key_field_name;
        let writer = state.writer.as_mut().expect("writer should not be null");

        let mut search_document = writer.schema.new_document();

        if writer.schema.has_nested() {
            let child_docs = row_to_search_documents(
                values,
                isnull,
                key_field_name,
                categorized_fields,
                &mut search_document,
                &writer.schema,
            )
            .unwrap_or_else(|err| {
                panic!(
                    "error creating nested index entries for index '{}': {err}",
                    CStr::from_ptr((*(*index_relation).rd_rel).relname.data.as_ptr())
                        .to_string_lossy()
                );
            });
            writer
                .insert_as_block(child_docs, search_document, item_pointer_to_u64(*ctid))
                .expect("insertion into index should succeed");
        } else {
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
                    CStr::from_ptr((*(*index_relation).rd_rel).relname.data.as_ptr())
                        .to_string_lossy()
                );
            });
            writer
                .insert(search_document, item_pointer_to_u64(*ctid))
                .expect("insertion into index should succeed");
        }
        true
    });

    match result {
        Ok(result) => result,
        Err(e) => resume_unwind(e),
    }
}

#[cfg(feature = "pg17")]
#[pg_guard]
pub unsafe extern "C" fn aminsertcleanup(
    _index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) {
    let state = (*index_info).ii_AmCache.cast::<InsertState>();
    if state.is_null() {
        return;
    }

    paradedb_aminsertcleanup(state.as_mut().and_then(|state| state.writer.take()));
}

pub fn paradedb_aminsertcleanup(mut writer: Option<SearchIndexWriter>) {
    if let Some(writer) = writer.take() {
        writer
            .commit()
            .expect("must be able to commit inserts in fake_aminsertcleanup");
    }
}
