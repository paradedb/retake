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

pub mod numeric;
pub mod string;

use crate::index::fast_fields_helper::{FFHelper, FastFieldType, WhichFastField};
use crate::index::reader::index::{SearchIndexReader, SearchResults};
use crate::index::BlockDirectoryType;
use crate::nodecast;
use crate::postgres::customscan::builders::custom_path::CustomPathBuilder;
use crate::postgres::customscan::builders::custom_state::{
    CustomScanStateBuilder, CustomScanStateWrapper,
};
use crate::postgres::customscan::explainer::Explainer;
use crate::postgres::customscan::pdbscan::exec_methods::fast_fields::numeric::NumericFastFieldExecState;
use crate::postgres::customscan::pdbscan::exec_methods::fast_fields::string::StringFastFieldExecState;
use crate::postgres::customscan::pdbscan::exec_methods::normal::NormalScanExecState;
use crate::postgres::customscan::pdbscan::privdat::PrivateData;
use crate::postgres::customscan::pdbscan::projections::score::{score_funcoid, uses_scores};
use crate::postgres::customscan::pdbscan::scan_state::PdbScanState;
use crate::postgres::customscan::pdbscan::PdbScan;
use crate::postgres::customscan::CustomScanState;
use crate::schema::SearchIndexSchema;
use itertools::Itertools;
use pgrx::{pg_sys, IntoDatum, PgList, PgOid, PgRelation, PgTupleDesc};
use tantivy::DocAddress;

pub struct FastFieldExecState {
    heaprel: pg_sys::Relation,
    tupdesc: Option<PgTupleDesc<'static>>,

    ffhelper: FFHelper,

    slot: *mut pg_sys::TupleTableSlot,
    strbuf: String,
    vmbuff: pg_sys::Buffer,
    which_fast_fields: Vec<WhichFastField>,
    search_results: SearchResults,

    did_query: bool,
}

impl Drop for FastFieldExecState {
    fn drop(&mut self) {
        unsafe {
            if pg_sys::IsTransactionState()
                && self.vmbuff != pg_sys::InvalidBuffer as pg_sys::Buffer
            {
                pg_sys::ReleaseBuffer(self.vmbuff);
            }
        }
    }
}

impl FastFieldExecState {
    pub fn new(which_fast_fields: Vec<WhichFastField>) -> Self {
        Self {
            heaprel: std::ptr::null_mut(),
            tupdesc: None,

            ffhelper: Default::default(),
            slot: std::ptr::null_mut(),
            strbuf: String::with_capacity(256),
            vmbuff: pg_sys::InvalidBuffer as pg_sys::Buffer,
            which_fast_fields,
            search_results: Default::default(),
            did_query: false,
        }
    }
}

#[inline(always)]
unsafe fn ff_to_datum(
    which_fast_field: (&WhichFastField, usize),
    typid: pg_sys::Oid,
    score: f32,
    doc_address: DocAddress,
    ff_helper: &mut FFHelper,
    strbuf: &mut String,
    slot: *const pg_sys::TupleTableSlot,
) -> Option<pg_sys::Datum> {
    let field_index = which_fast_field.1;
    let which_fast_field = which_fast_field.0;

    if typid == pg_sys::INT2OID || typid == pg_sys::INT4OID || typid == pg_sys::INT8OID {
        match ff_helper.i64(field_index, doc_address) {
            None => None,
            Some(v) => v.into_datum(),
        }
    } else if matches!(which_fast_field, WhichFastField::Ctid) {
        (*slot).tts_tid.into_datum()
    } else if matches!(which_fast_field, WhichFastField::TableOid) {
        (*slot).tts_tableOid.into_datum()
    } else if matches!(which_fast_field, WhichFastField::Score) {
        score.into_datum()
    } else if matches!(
        which_fast_field,
        WhichFastField::Named(_, FastFieldType::String)
    ) {
        strbuf.as_str().into_datum()
    } else if typid == pg_sys::TEXTOID || typid == pg_sys::VARCHAROID {
        // NB:  we don't actually support text-based fast fields... yet
        // but if we did, we'd want to do it this way
        ff_helper
            .string(field_index, doc_address, strbuf)
            .and_then(|s| strbuf.as_str().into_datum())
    } else {
        match ff_helper.value(field_index, doc_address) {
            None => None,
            Some(value) => value
                .try_into_datum(PgOid::from_untagged(typid))
                .expect("value should be convertible to Datum"),
        }
    }
}

/// Count how many "fast fields" are requested to be used by the query, as described by the `builder` argument.
pub unsafe fn count(
    builder: &mut CustomPathBuilder<PrivateData>,
    rti: pg_sys::Index,
    table: &PgRelation,
    schema: &SearchIndexSchema,
    target_list: *mut pg_sys::List,
) -> f64 {
    let ff = pullup_fast_fields(target_list, schema, table, rti).unwrap_or_default();

    builder.custom_private().set_maybe_ff(!ff.is_empty());
    ff.iter().sorted().dedup().count() as f64
}

/// Find all the fields that can be as "fast fields", categorize them as [`WhichFastField`]s, and
/// return the list.  If there are none, or one or more of the fields can't be used as a "fast field",
/// we return [`None`].
pub unsafe fn collect(
    maybe_ff: bool,
    target_list: *mut pg_sys::List,
    rti: pg_sys::Index,
    schema: &SearchIndexSchema,
    heaprel: &PgRelation,
) -> Option<Vec<WhichFastField>> {
    if maybe_ff {
        pullup_fast_fields(target_list, schema, heaprel, rti)
    } else {
        None
    }
}

pub unsafe fn pullup_fast_fields(
    node: *mut pg_sys::List,
    schema: &SearchIndexSchema,
    heaprel: &PgRelation,
    rti: pg_sys::Index,
) -> Option<Vec<WhichFastField>> {
    let mut matches = Vec::new();

    let tupdesc = heaprel.tuple_desc();
    let targetlist = PgList::<pg_sys::TargetEntry>::from_pg(node);
    for te in targetlist.iter_ptr() {
        if (*te).resorigtbl != pg_sys::Oid::INVALID && (*te).resorigtbl != heaprel.oid() {
            continue;
        }
        if let Some(var) = nodecast!(Var, T_Var, (*te).expr) {
            match (*var).varattno as i32 {
                // any of these mean we can't use fast fields
                pg_sys::MinTransactionIdAttributeNumber
                | pg_sys::MaxTransactionIdAttributeNumber
                | pg_sys::MinCommandIdAttributeNumber
                | pg_sys::MaxCommandIdAttributeNumber => return None,

                // these aren't _exactly_ fast fields, but we do have the information
                // readily available during the scan, so we'll pretend
                pg_sys::SelfItemPointerAttributeNumber => {
                    // okay, "ctid" is a fast field but it's secret
                    matches.push(WhichFastField::Ctid);
                    continue;
                }

                pg_sys::TableOidAttributeNumber => {
                    matches.push(WhichFastField::TableOid);
                    continue;
                }

                attno => {
                    let att = tupdesc
                        .get((attno - 1) as usize)
                        .expect("attnum should exist in tupdesc");
                    if schema.is_fast_field(att.name()) {
                        let ff_type = if (*var).vartype == pg_sys::TEXTOID
                            || (*var).vartype == pg_sys::VARCHAROID
                        {
                            FastFieldType::String
                        } else {
                            FastFieldType::Numeric
                        };

                        matches.push(WhichFastField::Named(att.name().to_string(), ff_type));
                        continue;
                    }
                }
            }
        } else if uses_scores((*te).expr.cast(), score_funcoid(), rti) {
            matches.push(WhichFastField::Score);
            continue;
        } else if pgrx::is_a((*te).expr.cast(), pg_sys::NodeTag::T_Aggref) {
            matches.push(WhichFastField::Junk("agg".into()));
            continue;
        } else if nodecast!(Const, T_Const, (*te).expr).is_some() {
            matches.push(WhichFastField::Junk("const".into()));
            continue;
        } else if nodecast!(WindowFunc, T_WindowFunc, (*te).expr).is_some() {
            matches.push(WhichFastField::Junk("window".into()));
            continue;
        }

        // we only support Vars or our score function in the target list
        return None;
    }

    if matches
        .iter()
        .sorted()
        .dedup()
        .filter(|ff| matches!(ff, WhichFastField::Named(_, FastFieldType::String)))
        .count()
        > 1
    {
        // we cannot support more than 1 different String fast field
        return None;
    }

    Some(matches)
}

/// If the query can return "fast fields", make that determination here, falling back to the
/// [`NormalScanExecState`] if not.
///
/// We support [`StringFastFieldExecState`] when there's 1 fast field and it's a string, or
/// [`NumericFastFieldExecState`] when there's one or more numeric fast fields
///
/// `paradedb.score()`, `ctid`, and `tableoid` are considered fast fields for the purposes of
/// these specialized [`ExecMethod`]s.
pub fn assign_exec_method(builder: &mut CustomScanStateBuilder<PdbScan, PrivateData>) {
    if let Some(field) = is_string_agg_capable(builder.custom_state()) {
        let which_fast_fields = builder.custom_state().which_fast_fields.clone().unwrap();
        builder
            .custom_state()
            .assign_exec_method(StringFastFieldExecState::new(field, which_fast_fields));
    } else if is_numeric_fast_field_capable(builder.custom_state()) {
        let which_fast_fields = builder.custom_state().which_fast_fields.clone().unwrap();
        builder
            .custom_state()
            .assign_exec_method(NumericFastFieldExecState::new(which_fast_fields));
    } else {
        builder
            .custom_state()
            .assign_exec_method(NormalScanExecState::default());
    }
}

fn is_string_agg_capable(state: &PdbScanState) -> Option<String> {
    is_string_agg_capable_ex(state.limit, &state.which_fast_fields)
}

pub fn is_string_agg_capable_ex(
    limit: Option<usize>,
    which_fast_fields: &Option<Vec<WhichFastField>>,
) -> Option<String> {
    if limit.is_some() {
        // doing a string_agg when there's a limit is always a loss, performance-wise
        return None;
    }
    let mut string_field = None;
    for ff in which_fast_fields.iter().flatten() {
        match ff {
            WhichFastField::Named(_, FastFieldType::String) if string_field.is_none() => {
                string_field = Some(ff.name())
            }
            WhichFastField::Named(_, FastFieldType::String) => {
                // too many string fields for us to be capable of doing a string_agg
                return None;
            }
            _ => {
                // noop
            }
        }
    }
    string_field
}

fn is_numeric_fast_field_capable(state: &PdbScanState) -> bool {
    if state.which_fast_fields.is_none() || state.targetlist_len == 0 {
        return false;
    }

    for ff in state.which_fast_fields.iter().flatten() {
        if matches!(ff, WhichFastField::Named(_, FastFieldType::String)) {
            return false;
        }
    }
    true
}

/// Add nodes to `EXPLAIN` output to describe the "fast fields" being used by the query, if any
pub fn explain(state: &CustomScanStateWrapper<PdbScan>, explainer: &mut Explainer) {
    if state.custom_state().is_top_n_capable().is_none() {
        if let Some(fast_fields) = state.custom_state().which_fast_fields.as_ref() {
            explainer.add_text(
                "Fast Fields",
                fast_fields
                    .iter()
                    .map(|field| field.name())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            if let Some(string_agg_field) = is_string_agg_capable(state.custom_state()) {
                explainer.add_text("String Agg Field", string_agg_field);
            }
        }
    }
}

pub fn estimate_cardinality(indexrel: &PgRelation, field: &str) -> Option<usize> {
    let reader = SearchIndexReader::open(indexrel, BlockDirectoryType::Mvcc, false)
        .expect("estimate_cardinality: should be able to open SearchIndexReader");
    let searcher = reader.searcher();
    let largest_segment_reader = searcher
        .segment_readers()
        .iter()
        .max_by_key(|sr| sr.num_docs())
        .unwrap();

    Some(
        largest_segment_reader
            .fast_fields()
            .str(field)
            .ok()
            .flatten()?
            .num_terms(),
    )
}
