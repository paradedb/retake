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

pub(crate) mod fast_fields;
pub(crate) mod normal;
pub(crate) mod top_n;

use crate::postgres::customscan::pdbscan::scan_state::PdbScanState;
use pgrx::pg_sys;
use tantivy::{DocAddress, Score};

pub enum ExecState {
    RequiresVisibilityCheck {
        ctid: u64,
        score: Score,
        doc_address: DocAddress,
    },
    Virtual {
        slot: *mut pg_sys::TupleTableSlot,
    },
    Eof,
}

impl Default for Box<dyn ExecMethod> {
    fn default() -> Self {
        Box::new(UnknownScanStyle)
    }
}

pub trait ExecMethod {
    fn init(&mut self, state: &mut PdbScanState, cstate: *mut pg_sys::CustomScanState);

    fn query(&mut self, state: &PdbScanState) -> bool {
        false
    }

    fn next(&mut self, state: &PdbScanState) -> ExecState {
        loop {
            match self.internal_next() {
                ExecState::Eof => {
                    if !self.query(state) {
                        return ExecState::Eof;
                    }
                }
                other => return other,
            }
        }
    }

    fn internal_next(&mut self) -> ExecState;
}

struct UnknownScanStyle;

impl ExecMethod for UnknownScanStyle {
    fn init(&mut self, _state: &mut PdbScanState, _cstate: *mut pg_sys::CustomScanState) {
        unimplemented!(
            "logic error in pg_search:  `UnknownScanStyle::init()` should never be called"
        )
    }

    fn internal_next(&mut self) -> ExecState {
        unimplemented!(
            "logic error in pg_search:  `UnknownScanStyle::internal_next()` should never be called"
        )
    }
}
