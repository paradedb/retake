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
#![allow(unpredictable_function_pointer_comparisons)]
use crate::postgres::parallel::Spinlock;
use crate::query::SearchQueryInput;
use pgrx::*;
use std::io::Write;
use tantivy::index::SegmentId;
use tantivy::SegmentReader;

mod build;
mod cost;
mod delete;
mod insert;
pub mod options;
mod range;
mod scan;
mod vacuum;
mod validate;

pub mod customscan;
pub mod datetime;
#[cfg(not(feature = "pg17"))]
pub mod fake_aminsertcleanup;
pub mod index;
mod parallel;
pub mod storage;
pub mod types;
pub mod utils;
pub mod visibility_checker;

#[repr(u16)] // b/c that's what [`pg_sys::StrategyNumber`] is
pub enum ScanStrategy {
    TextQuery = 1,
    SearchQueryInput = 2,
    // NB:  Any additions here **mut** update the `amroutine.amstrategies` down below in [`bm25_handler`]
}

impl TryFrom<pg_sys::StrategyNumber> for ScanStrategy {
    type Error = String;

    fn try_from(value: pg_sys::StrategyNumber) -> Result<Self, Self::Error> {
        if value == 1 {
            Ok(ScanStrategy::TextQuery)
        } else if value == 2 {
            Ok(ScanStrategy::SearchQueryInput)
        } else {
            Err(format!("`{value}` is an unknown `ScanStrategy` number"))
        }
    }
}

#[pg_extern(sql = "
CREATE FUNCTION bm25_handler(internal) RETURNS index_am_handler PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
CREATE ACCESS METHOD bm25 TYPE INDEX HANDLER bm25_handler;
COMMENT ON ACCESS METHOD bm25 IS 'bm25 index access method';
")]
fn bm25_handler(_fcinfo: pg_sys::FunctionCallInfo) -> PgBox<pg_sys::IndexAmRoutine> {
    let mut amroutine =
        unsafe { PgBox::<pg_sys::IndexAmRoutine>::alloc_node(pg_sys::NodeTag::T_IndexAmRoutine) };

    amroutine.amstrategies = 2;
    amroutine.amsupport = 0;
    amroutine.amcanmulticol = true;
    amroutine.amsearcharray = true;

    amroutine.amkeytype = pg_sys::InvalidOid;

    amroutine.amvalidate = Some(validate::amvalidate);
    amroutine.ambuild = Some(build::ambuild);
    amroutine.ambuildempty = Some(build::ambuildempty);
    amroutine.aminsert = Some(insert::aminsert);
    #[cfg(feature = "pg17")]
    {
        amroutine.aminsertcleanup = Some(insert::aminsertcleanup);
    }
    amroutine.ambulkdelete = Some(delete::ambulkdelete);
    amroutine.amvacuumcleanup = Some(vacuum::amvacuumcleanup);
    amroutine.amcostestimate = Some(cost::amcostestimate);
    amroutine.amoptions = Some(options::amoptions);
    amroutine.ambeginscan = Some(scan::ambeginscan);
    amroutine.amrescan = Some(scan::amrescan);
    amroutine.amgettuple = Some(scan::amgettuple);
    amroutine.amgetbitmap = Some(scan::amgetbitmap);
    amroutine.amendscan = Some(scan::amendscan);
    amroutine.amcanreturn = Some(scan::amcanreturn);

    amroutine.amcanparallel = true;
    amroutine.aminitparallelscan = Some(parallel::aminitparallelscan);
    amroutine.amestimateparallelscan = Some(parallel::amestimateparallelscan);
    amroutine.amparallelrescan = Some(parallel::amparallelrescan);

    amroutine.into_pg_boxed()
}

pub fn rel_get_bm25_index(relid: pg_sys::Oid) -> Option<(PgRelation, PgRelation)> {
    unsafe {
        let rel = PgRelation::with_lock(relid, pg_sys::AccessShareLock as _);
        for index in rel.indices(pg_sys::AccessShareLock as _) {
            if (*index.rd_indam).ambuild == Some(build::ambuild) {
                return Some((rel, index));
            }
        }
        None
    }
}

#[repr(C, packed)]
struct ParallelScanPayload {
    query: (usize, usize),
    segments: (usize, usize),
    data: [u8; 0], // dynamically sized, allocated after
}

impl ParallelScanPayload {
    fn init(&mut self, segments: &[SegmentReader], query: &[u8]) {
        unsafe {
            self.query = (0, query.len());
            self.segments = (self.query.1, self.query.1 + segments.len() * 16);

            let query_start = self.query.0;
            let query_end = self.query.1;
            let _ = (&mut self.data_mut()[query_start..query_end])
                .write(query)
                .expect("failed to write query bytes");

            let segments_start = self.segments.0;
            let segments_end = self.segments.1;
            let ptr = &mut self.data_mut()[segments_start..segments_end].as_mut_ptr();
            let segments_slice: &mut [[u8; 16]] =
                std::slice::from_raw_parts_mut(ptr.cast(), segments.len());

            for (segment, target) in segments.iter().zip(segments_slice.iter_mut()) {
                let _ = (&mut target[..])
                    .write(segment.segment_id().uuid_bytes())
                    .expect("failed to write segment bytes");
            }
        }
    }

    #[inline(always)]
    fn data(&self) -> &[u8] {
        assert!(self.segments.1 > 0);
        unsafe {
            let data_end = self.segments.1;
            let data_ptr = self.data.as_ptr();
            std::slice::from_raw_parts(data_ptr, data_end)
        }
    }

    #[inline(always)]
    fn data_mut(&mut self) -> &mut [u8] {
        assert!(self.segments.1 > 0);
        unsafe {
            let data_end = self.segments.1;
            let data_ptr = self.data.as_mut_ptr();
            std::slice::from_raw_parts_mut(data_ptr, data_end)
        }
    }

    fn query(&self) -> anyhow::Result<Option<SearchQueryInput>> {
        let query_start = self.query.0;
        let query_end = self.query.1;
        if query_end == 0 {
            return Ok(None);
        }
        let query_data = &self.data()[query_start..query_end];
        Ok(Some(serde_json::from_slice(query_data)?))
    }

    fn segments(&self) -> &[[u8; 16]] {
        let segments_start = self.segments.0;
        let segments_end = self.segments.1;
        let segments_data = &self.data()[segments_start..segments_end];
        assert!(
            segments_data.len() % 16 == 0,
            "segment data length mismatch"
        );

        unsafe { std::mem::transmute(segments_data) }
    }
}

#[repr(C)]
pub struct ParallelScanState {
    mutex: Spinlock,
    remaining_segments: usize,
    payload: ParallelScanPayload, // must be last field, b/c it allocates on the heap after this struct
}

impl ParallelScanState {
    #[inline]
    fn size_of(nsegments: usize, serialized_query: &[u8]) -> usize {
        // a SegmentId, in byte form, is 16 bytes
        size_of::<Self>() + size_of::<Self>() + (nsegments * 16) + serialized_query.len()
    }

    fn init(&mut self, segments: &[SegmentReader], query: &[u8]) {
        self.mutex.init();
        self.init_without_mutex(segments, query);
    }

    fn init_without_mutex(&mut self, segments: &[SegmentReader], query: &[u8]) {
        self.payload.init(segments, query);
        self.remaining_segments = segments.len();
    }

    fn init_mutex(&mut self) {
        self.mutex.init();
    }

    pub fn acquire_mutex(&mut self) -> impl Drop {
        self.mutex.acquire()
    }

    pub fn remaining_segments(&self) -> usize {
        self.remaining_segments
    }

    pub fn decrement_remaining_segments(&mut self) -> usize {
        self.remaining_segments -= 1;
        self.remaining_segments
    }

    fn segment_id(&self, i: usize) -> SegmentId {
        SegmentId::from_bytes(self.payload.segments()[i])
    }

    fn query(&self) -> anyhow::Result<Option<SearchQueryInput>> {
        self.payload.query()
    }
}
