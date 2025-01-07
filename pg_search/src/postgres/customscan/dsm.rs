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

use crate::postgres::customscan::builders::custom_state::CustomScanStateWrapper;
use crate::postgres::customscan::exec::{
    begin_custom_scan, end_custom_scan, exec_custom_scan, explain_custom_scan, rescan_custom_scan,
    shutdown_custom_scan,
};
use crate::postgres::customscan::{wrap_custom_scan_state, CustomScan, ExecMethod};
use pgrx::{pg_guard, pg_sys, PgMemoryContexts};

pub trait ParallelQueryCapable: ExecMethod
where
    Self: CustomScan,
{
    fn exec_methods() -> *const pg_sys::CustomExecMethods {
        unsafe {
            static mut METHODS: *mut pg_sys::CustomExecMethods = std::ptr::null_mut();

            if METHODS.is_null() {
                METHODS = PgMemoryContexts::TopMemoryContext.leak_and_drop_on_delete(
                    pg_sys::CustomExecMethods {
                        CustomName: Self::NAME.as_ptr(),
                        BeginCustomScan: Some(begin_custom_scan::<Self>),
                        ExecCustomScan: Some(exec_custom_scan::<Self>),
                        EndCustomScan: Some(end_custom_scan::<Self>),
                        ReScanCustomScan: Some(rescan_custom_scan::<Self>),
                        MarkPosCustomScan: None,
                        RestrPosCustomScan: None,
                        EstimateDSMCustomScan: Some(estimate_dsm_custom_scan::<Self>),
                        InitializeDSMCustomScan: Some(initialize_dsm_custom_scan::<Self>),
                        ReInitializeDSMCustomScan: Some(reinitialize_dsm_custom_scan::<Self>),
                        InitializeWorkerCustomScan: Some(initialize_worker_custom_scan::<Self>),
                        ShutdownCustomScan: Some(shutdown_custom_scan::<Self>),
                        ExplainCustomScan: Some(explain_custom_scan::<Self>),
                    },
                );
            }
            METHODS
        }
    }

    fn estimate_dsm_custom_scan(
        state: &mut CustomScanStateWrapper<Self>,
        pcxt: *mut pg_sys::ParallelContext,
    ) -> pg_sys::Size;

    fn initialize_dsm_custom_scan(
        state: &mut CustomScanStateWrapper<Self>,
        pcxt: *mut pg_sys::ParallelContext,
        coordinate: *mut std::os::raw::c_void,
    );

    fn reinitialize_dsm_custom_scan(
        state: &mut CustomScanStateWrapper<Self>,
        pcxt: *mut pg_sys::ParallelContext,
        coordinate: *mut std::os::raw::c_void,
    );

    fn initialize_worker_custom_scan(
        state: &mut CustomScanStateWrapper<Self>,
        toc: *mut pg_sys::shm_toc,
        coordinate: *mut std::os::raw::c_void,
    );
}

/// Estimate the amount of dynamic shared memory that will be required for parallel operation. This
/// may be higher than the amount that will actually be used, but it must not be lower. The return
/// value is in bytes. This callback is optional, and need only be supplied if this custom scan
/// provider supports parallel execution.
#[pg_guard]
pub extern "C" fn estimate_dsm_custom_scan<CS: CustomScan + ParallelQueryCapable>(
    node: *mut pg_sys::CustomScanState,
    pcxt: *mut pg_sys::ParallelContext,
) -> pg_sys::Size {
    let mut custom_state = wrap_custom_scan_state::<CS>(node);
    unsafe { CS::estimate_dsm_custom_scan(custom_state.as_mut(), pcxt) }
}

/// Initialize the dynamic shared memory that will be required for parallel operation. coordinate
/// points to a shared memory area of size equal to the return value of EstimateDSMCustomScan. This
/// callback is optional, and need only be supplied if this custom scan provider supports parallel
/// execution.
#[pg_guard]
pub extern "C" fn initialize_dsm_custom_scan<CS: CustomScan + ParallelQueryCapable>(
    node: *mut pg_sys::CustomScanState,
    pcxt: *mut pg_sys::ParallelContext,
    coordinate: *mut std::os::raw::c_void,
) {
    let mut custom_state = wrap_custom_scan_state::<CS>(node);
    unsafe { CS::initialize_dsm_custom_scan(custom_state.as_mut(), pcxt, coordinate) }
}

/// Re-initialize the dynamic shared memory required for parallel operation when the custom-scan
/// plan node is about to be re-scanned. This callback is optional, and need only be supplied if
/// this custom scan provider supports parallel execution. Recommended practice is that this callback
/// reset only shared state, while the ReScanCustomScan callback resets only local state. Currently,
/// this callback will be called before ReScanCustomScan, but it's best not to rely on that ordering.
#[pg_guard]
pub extern "C" fn reinitialize_dsm_custom_scan<CS: CustomScan + ParallelQueryCapable>(
    node: *mut pg_sys::CustomScanState,
    pcxt: *mut pg_sys::ParallelContext,
    coordinate: *mut std::os::raw::c_void,
) {
    let mut custom_state = wrap_custom_scan_state::<CS>(node);
    unsafe { CS::reinitialize_dsm_custom_scan(custom_state.as_mut(), pcxt, coordinate) }
}

/// Initialize a parallel worker's local state based on the shared state set up by the leader during
/// InitializeDSMCustomScan. This callback is optional, and need only be supplied if this custom scan
/// provider supports parallel execution.
#[pg_guard]
pub extern "C" fn initialize_worker_custom_scan<CS: CustomScan + ParallelQueryCapable>(
    node: *mut pg_sys::CustomScanState,
    toc: *mut pg_sys::shm_toc,
    coordinate: *mut std::os::raw::c_void,
) {
    let mut custom_state = wrap_custom_scan_state::<CS>(node);
    unsafe { CS::initialize_worker_custom_scan(custom_state.as_mut(), toc, coordinate) }
}
