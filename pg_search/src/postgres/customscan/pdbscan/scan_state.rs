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

use crate::index::fast_fields_helper::WhichFastField;
use crate::index::reader::index::{SearchIndexReader, SearchResults};
use crate::postgres::customscan::builders::custom_path::SortDirection;
use crate::postgres::customscan::pdbscan::exec_methods::ExecMethod;
use crate::postgres::customscan::pdbscan::projections::snippet::SnippetInfo;
use crate::postgres::customscan::pdbscan::qual_inspect::Qual;
use crate::postgres::customscan::CustomScanState;
use crate::postgres::options::SearchIndexCreateOptions;
use crate::postgres::utils::u64_to_item_pointer;
use crate::postgres::visibility_checker::VisibilityChecker;
use crate::postgres::ParallelScanState;
use crate::query::SearchQueryInput;
use pgrx::heap_tuple::PgHeapTuple;
use pgrx::{name_data_to_str, pg_sys, PgRelation, PgTupleDesc};
use std::cell::UnsafeCell;
use std::collections::HashMap;
use tantivy::snippet::SnippetGenerator;

#[derive(Default)]
pub struct PdbScanState {
    pub parallel_state: Option<*mut ParallelScanState>,

    pub rti: pg_sys::Index,

    pub search_query_input: SearchQueryInput,
    pub serialized_query: Vec<u8>,
    pub nexprs: usize,
    pub search_reader: Option<SearchIndexReader>,

    pub search_results: SearchResults,
    pub which_fast_fields: Option<Vec<WhichFastField>>,
    pub targetlist_len: usize,

    pub limit: Option<usize>,
    pub sort_field: Option<String>,
    pub sort_direction: Option<SortDirection>,
    pub retry_count: usize,
    pub heap_tuple_check_count: usize,
    pub virtual_tuple_count: usize,
    pub invisible_tuple_count: usize,

    pub heaprelid: pg_sys::Oid,
    pub heaprel: Option<pg_sys::Relation>,
    pub indexrel: Option<pg_sys::Relation>,
    pub indexrelid: pg_sys::Oid,
    pub lockmode: pg_sys::LOCKMODE,

    pub heaprel_namespace: String,
    pub heaprel_relname: String,

    pub visibility_checker: Option<VisibilityChecker>,
    pub segment_count: usize,
    pub quals: Option<Qual>,

    pub need_scores: bool,
    pub const_score_node: Option<*mut pg_sys::Const>,
    pub score_funcoid: pg_sys::Oid,

    pub const_snippet_nodes: HashMap<SnippetInfo, Vec<*mut pg_sys::Const>>,
    pub snippet_funcoid: pg_sys::Oid,
    pub snippet_generators:
        HashMap<SnippetInfo, Option<(tantivy::schema::Field, SnippetGenerator)>>,
    pub var_attname_lookup: HashMap<(i32, pg_sys::AttrNumber), String>,

    pub placeholder_targetlist: Option<*mut pg_sys::List>,

    exec_method: UnsafeCell<Box<dyn ExecMethod>>,
    exec_method_name: String,
}

impl CustomScanState for PdbScanState {
    fn init_exec_method(&mut self, cstate: *mut pg_sys::CustomScanState) {
        unsafe {
            // SAFETY: inner_scan_state is always initialized and call to `init()` could never move `self`
            (*self.exec_method.get()).init(self, cstate)
        }
    }

    fn is_top_n_capable(&self) -> Option<(usize, SortDirection)> {
        match (self.limit, self.sort_direction) {
            (Some(limit), Some(sort_direction)) => Some((limit, sort_direction)),
            _ => None,
        }
    }

    fn is_unsorted_top_n_capable(&self) -> Option<usize> {
        match (self.limit, self.sort_direction) {
            (Some(limit), Some(SortDirection::None)) => Some(limit),
            _ => None,
        }
    }
}

impl PdbScanState {
    #[inline(always)]
    pub fn assign_exec_method<T: ExecMethod + 'static>(&mut self, method: T) {
        self.exec_method = UnsafeCell::new(Box::new(method));
        self.exec_method_name = std::any::type_name::<T>().to_string();
    }

    #[inline(always)]
    pub fn exec_method<'a>(&self) -> &'a dyn ExecMethod {
        let ptr = self.exec_method.get();
        assert!(!ptr.is_null());
        unsafe { ptr.as_ref().unwrap_unchecked().as_ref() }
    }

    #[inline(always)]
    pub fn exec_method_mut<'a>(&mut self) -> &'a mut Box<dyn ExecMethod> {
        let ptr = self.exec_method.get();
        assert!(!ptr.is_null());
        unsafe { ptr.as_mut().unwrap_unchecked() }
    }

    pub fn exec_method_name(&self) -> &str {
        &self.exec_method_name
    }

    #[inline(always)]
    pub fn need_scores(&self) -> bool {
        self.need_scores
            || self.search_query_input.need_scores()
            || self
                .quals
                .as_ref()
                .map(|quals| quals.contains_score_exprs())
                .unwrap_or_default()
    }

    #[inline(always)]
    pub fn determine_key_field(&self) -> String {
        unsafe {
            let indexrel = PgRelation::with_lock(self.indexrelid, pg_sys::AccessShareLock as _);
            let ops = indexrel.rd_options as *mut SearchIndexCreateOptions;
            (*ops)
                .get_key_field()
                .expect("`USING bm25` index should have a valued `key_field` option")
                .0
        }
    }

    #[inline(always)]
    pub fn need_snippets(&self) -> bool {
        !self.snippet_generators.is_empty()
    }

    #[track_caller]
    #[inline(always)]
    pub fn heaprel(&self) -> pg_sys::Relation {
        self.heaprel.unwrap()
    }

    #[inline(always)]
    pub fn indexrel(&self) -> pg_sys::Relation {
        self.indexrel.unwrap()
    }

    #[inline(always)]
    pub fn heaprel_namespace(&self) -> &str {
        &self.heaprel_namespace
    }

    #[inline(always)]
    pub fn heaprelname(&self) -> &str {
        &self.heaprel_relname
    }

    #[inline(always)]
    pub fn indexrelname(&self) -> &str {
        unsafe { name_data_to_str(&(*(*self.indexrel()).rd_rel).relname) }
    }

    #[inline(always)]
    pub fn heaptupdesc(&self) -> pg_sys::TupleDesc {
        unsafe { (*self.heaprel()).rd_att }
    }

    #[inline(always)]
    pub fn visibility_checker(&mut self) -> &mut VisibilityChecker {
        self.visibility_checker.as_mut().unwrap()
    }

    pub fn make_snippet(&self, ctid: u64, snippet_info: &SnippetInfo) -> Option<String> {
        let heaprel = self
            .heaprel
            .expect("make_snippet: heaprel should be initialized");
        let mut ipd = pg_sys::ItemPointerData::default();
        u64_to_item_pointer(ctid, &mut ipd);

        let text = unsafe {
            let mut htup = pg_sys::HeapTupleData {
                t_self: ipd,
                ..Default::default()
            };
            let mut buffer: pg_sys::Buffer = pg_sys::InvalidBuffer as i32;

            #[cfg(any(feature = "pg13", feature = "pg14"))]
            {
                if !pg_sys::heap_fetch(heaprel, pg_sys::GetActiveSnapshot(), &mut htup, &mut buffer)
                {
                    return None;
                }
            }

            #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
            {
                if !pg_sys::heap_fetch(
                    heaprel,
                    pg_sys::GetActiveSnapshot(),
                    &mut htup,
                    &mut buffer,
                    false,
                ) {
                    return None;
                }
            }

            pg_sys::ReleaseBuffer(buffer);

            let tuple_desc = PgTupleDesc::from_pg_unchecked((*heaprel).rd_att);
            let heap_tuple = PgHeapTuple::from_heap_tuple(tuple_desc.clone(), &mut htup);
            let (index, attribute) = heap_tuple
                .get_attribute_by_name(&snippet_info.field)
                .unwrap();

            if pg_sys::type_is_array(attribute.type_oid().value()) {
                // varchar[] and text[] are flattened into a single string
                // to emulate Tantivy's default behavior for highlighting text arrays
                pgrx::htup::heap_getattr::<Vec<Option<String>>, _>(
                    &pgrx::pgbox::PgBox::from_pg(&mut htup),
                    index,
                    &tuple_desc,
                )
                .unwrap_or_default()
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join(" ")
            } else {
                heap_tuple
                    .get_by_name(&snippet_info.field)
                    .unwrap_or_else(|_| {
                        panic!("{} should exist in the heap tuple", snippet_info.field)
                    })
                    .unwrap_or_default()
            }
        };

        let (field, generator) = self.snippet_generators.get(snippet_info)?.as_ref()?;
        let mut snippet = generator.snippet(&text);

        snippet.set_snippet_prefix_postfix(&snippet_info.start_tag, &snippet_info.end_tag);
        let html = snippet.to_html();
        if html.trim().is_empty() {
            None
        } else {
            Some(html)
        }
    }
}
