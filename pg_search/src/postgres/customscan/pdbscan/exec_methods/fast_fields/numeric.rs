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

use crate::index::fast_fields_helper::{FFHelper, WhichFastField};
use crate::index::reader::index::SearchResults;
use crate::postgres::customscan::pdbscan::exec_methods::fast_fields::{
    ff_to_datum, FastFieldExecState,
};
use crate::postgres::customscan::pdbscan::exec_methods::{ExecMethod, ExecState};
use crate::postgres::customscan::pdbscan::is_block_all_visible;
use crate::postgres::customscan::pdbscan::parallel::checkout_segment;
use crate::postgres::customscan::pdbscan::scan_state::PdbScanState;
use pgrx::pg_sys::CustomScanState;
use pgrx::{pg_sys, PgTupleDesc};

pub struct NumericFastFieldExecState {
    inner: FastFieldExecState,
}

impl NumericFastFieldExecState {
    pub fn new(which_fast_fields: Vec<WhichFastField>) -> Self {
        Self {
            inner: FastFieldExecState::new(which_fast_fields),
        }
    }
}

impl ExecMethod for NumericFastFieldExecState {
    fn init(&mut self, state: &mut PdbScanState, cstate: *mut CustomScanState) {
        unsafe {
            self.inner.heaprel = state.heaprel();
            self.inner.tupdesc = Some(PgTupleDesc::from_pg_unchecked(
                (*cstate).ss.ps.ps_ResultTupleDesc,
            ));
            self.inner.slot = pg_sys::MakeTupleTableSlot(
                (*cstate).ss.ps.ps_ResultTupleDesc,
                &pg_sys::TTSOpsVirtual,
            );
            self.inner.ffhelper = FFHelper::with_fields(
                state.search_reader.as_ref().unwrap(),
                &self.inner.which_fast_fields,
            );
        }
    }

    fn query(&mut self, state: &PdbScanState) -> bool {
        if let Some(parallel_state) = state.parallel_state {
            if let Some(segment_ord) = unsafe { checkout_segment(parallel_state) } {
                self.inner.search_results = state.search_reader.as_ref().unwrap().search_segment(
                    state.need_scores(),
                    segment_ord,
                    &state.search_query_input,
                );
                return true;
            }

            // no more segments to query
            self.inner.search_results = SearchResults::None;
            false
        } else if self.inner.did_query {
            // not parallel, so we're done
            false
        } else {
            // not parallel, first time query
            self.inner.search_results = state.search_reader.as_ref().unwrap().search(
                state.need_scores(),
                false,
                &state.search_query_input,
                state.limit,
            );
            self.inner.did_query = true;
            true
        }
    }

    fn internal_next(&mut self) -> ExecState {
        unsafe {
            match self.inner.search_results.next() {
                None => ExecState::Eof,
                Some((scored, doc_address)) => {
                    let slot = self.inner.slot;
                    let natts = (*(*slot).tts_tupleDescriptor).natts as usize;

                    crate::postgres::utils::u64_to_item_pointer(scored.ctid, &mut (*slot).tts_tid);
                    (*slot).tts_tableOid = (*self.inner.heaprel).rd_id;

                    if is_block_all_visible(
                        self.inner.heaprel,
                        &mut self.inner.vmbuff,
                        (*slot).tts_tid,
                        (*slot).tts_tableOid,
                    ) {
                        (*slot).tts_flags &= !pg_sys::TTS_FLAG_EMPTY as u16;
                        (*slot).tts_flags |= pg_sys::TTS_FLAG_SHOULDFREE as u16;
                        (*slot).tts_nvalid = natts as _;

                        let datums = std::slice::from_raw_parts_mut((*slot).tts_values, natts);
                        let isnull = std::slice::from_raw_parts_mut((*slot).tts_isnull, natts);

                        #[rustfmt::skip]
                        debug_assert!(natts == self.inner.which_fast_fields.len());

                        let fast_fields = &mut self.inner.ffhelper;
                        let which_fast_fields = &self.inner.which_fast_fields;
                        for (i, att) in self.inner.tupdesc.as_ref().unwrap().iter().enumerate() {
                            let which_fast_field = &which_fast_fields[i];

                            match ff_to_datum(
                                (which_fast_field, i),
                                att.atttypid,
                                scored.bm25,
                                doc_address,
                                fast_fields,
                                &mut self.inner.strbuf,
                                slot,
                            ) {
                                None => {
                                    datums[i] = pg_sys::Datum::null();
                                    isnull[i] = true;
                                }
                                Some(datum) => {
                                    datums[i] = datum;
                                    isnull[i] = false;
                                }
                            }
                        }

                        ExecState::Virtual { slot }
                    } else {
                        ExecState::RequiresVisibilityCheck {
                            ctid: scored.ctid,
                            score: scored.bm25,
                            doc_address,
                        }
                    }
                }
            }
        }
    }
}
