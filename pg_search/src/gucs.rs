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

use crate::index::Parallelism;
use pgrx::{pg_sys, GucContext, GucFlags, GucRegistry, GucSetting};
use std::num::NonZeroUsize;

/// Is our telemetry tracking enabled?  Default is `true`.
static TELEMETRY: GucSetting<bool> = GucSetting::<bool>::new(cfg!(feature = "telemetry"));

/// Allows the user to toggle the use of our "ParadeDB Custom Scan".  The default is `true`.
static ENABLE_CUSTOM_SCAN: GucSetting<bool> = GucSetting::<bool>::new(true);

/// The `PER_TUPLE_COST` is an arbitrary value that needs to be really high.  In fact, we default
/// to one hundred million.
///
/// The reason for this is we really do not want Postgres to choose a plan where the `@@@` operator
/// is used in a sequential scan, filter, or recheck condition... unless of course there's no
/// other way to solve the query.
///
/// This value is a multiplier that Postgres applies to the estimated row count any given `@@@`
/// query clause will return.  In our case, higher is better.
///
/// Our IAM impl has its own costing functions that don't use this GUC and provide sensible estimates
/// for the overall IndexScan.  That plus this help to persuade Postgres to use our IAM whenever
/// it logically can.
static PER_TUPLE_COST: GucSetting<f64> = GucSetting::<f64>::new(100_000_000.0);

/// Should we log the progress of the CREATE INDEX operation?  Default is `false`.
static LOG_CREATE_INDEX_PROGRESS: GucSetting<bool> = GucSetting::<bool>::new(false);

/// How many threads should tantivy use during CREATE INDEX?
static CREATE_INDEX_PARALLELISM: GucSetting<i32> = GucSetting::<i32>::new(0);

/// How much memory should tantivy use during CREATE INDEX.  This value is decided to each indexing
/// thread.  So if there's 10 threads and this value is 100MB, then a total of 1GB will be allocated.
static CREATE_INDEX_MEMORY_BUDGET: GucSetting<i32> = GucSetting::<i32>::new(1024);

/// How many threads should tantivy use during a regular INSERT/UPDATE/COPY statement?
static STATEMENT_PARALLELISM: GucSetting<i32> = GucSetting::<i32>::new(1);

/// How much memory should tantivy use during a regular INSERT/UPDATE/COPY statement?  This value is decided to each indexing
/// thread.  So if there's 10 threads and this value is 100MB, then a total of 1GB will be allocated.
static STATEMENT_MEMORY_BUDGET: GucSetting<i32> = GucSetting::<i32>::new(1024);

/// Segments whose estimated byte size is larger than this value will **NOT** be considered for merging
/// The default is 200MB
static MAX_MERGEABLE_SEGMENT_SIZE: GucSetting<i32> = GucSetting::<i32>::new(200 * 1024 * 1024);

pub fn init() {
    // Note that Postgres is very specific about the naming convention of variables.
    // They must be namespaced... we use 'paradedb.<variable>' below.
    // They cannot have more than one '.' - paradedb.pg_search.telemetry will not work.

    let (telemetry_context, telemetry_flags) = if cfg!(feature = "telemetry") {
        (GucContext::Userset, GucFlags::default())
    } else {
        (GucContext::Internal, GucFlags::DISALLOW_IN_FILE)
    };

    GucRegistry::define_bool_guc(
        "paradedb.pg_search_telemetry",
        "Enable telemetry on the ParadeDB pg_search extension.",
        "Enable telemetry on the ParadeDB pg_search extension.",
        &TELEMETRY,
        telemetry_context,
        telemetry_flags,
    );

    GucRegistry::define_bool_guc(
        "paradedb.enable_custom_scan",
        "Enable ParadeDB's custom scan",
        "Enable ParadeDB's custom scan",
        &ENABLE_CUSTOM_SCAN,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        "paradedb.per_tuple_cost",
        "Arbitrary multiplier for the cost of retrieving a tuple from a USING bm25 index outside of an IndexScan",
        "Default is 100,000,000.0.  It is very expensive to use a USING bm25 index in the wrong query plan",
        &PER_TUPLE_COST,
        0.0,
        f64::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "paradedb.log_create_index_progress",
        "Log CREATE INDEX progress every 100,000 rows",
        "",
        &LOG_CREATE_INDEX_PROGRESS,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "paradedb.create_index_parallelism",
        "The number of threads to use when creating an index",
        "Default is 0, which means a thread for as many cores in the machine",
        &CREATE_INDEX_PARALLELISM,
        0,
        std::thread::available_parallelism()
            .expect("your computer should have at least one core")
            .get()
            .try_into()
            .expect("your computer has too many cores"),
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "paradedb.create_index_memory_budget",
        "The amount of memory to allocate to 1 thread during indexing",
        "Default is `1GB`, which is allocated to each thread defined by `paradedb.create_index_parallelism`",
        &CREATE_INDEX_MEMORY_BUDGET,
        0,
        i32::MAX,
        GucContext::Userset,
        GucFlags::UNIT_MB,
    );

    GucRegistry::define_int_guc(
        "paradedb.statement_parallelism",
        "The number of threads to use when indexing during an INSERT/UPDATE/COPY statement",
        "Default is 1.  Recommended value is generally 1.  Value of zero means a thread for as many cores in the machine",
        &STATEMENT_PARALLELISM,
        0,
        std::thread::available_parallelism().expect("your computer should have at least one core").get().try_into().expect("your computer has too many cores"),
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "paradedb.statement_memory_budget",
        "The amount of memory to allocate to 1 thread during an INSERT/UPDATE/COPY statement",
        "Default is `1GB`, which is allocated to each thread defined by `paradedb.statement_parallelism`",
        &STATEMENT_MEMORY_BUDGET,
        0,
        i32::MAX,
        GucContext::Userset,
        GucFlags::UNIT_MB,
    );

    GucRegistry::define_int_guc(
        "paradedb.max_mergeable_segment_size",
        "If the estimated byte size of a segment is greater than this value, then it will NOT be merged with the next segment",
        "Default is `200MB`",
        &MAX_MERGEABLE_SEGMENT_SIZE,
        0,
        i32::MAX,
        GucContext::Userset,
        GucFlags::UNIT_BYTE,
    );
}

pub fn telemetry_enabled() -> bool {
    // If PARADEDB_TELEMETRY is not 'true' at compile time, then we will never enable.
    // This is useful for test builds and CI.
    option_env!("PARADEDB_TELEMETRY") == Some("true") && TELEMETRY.get()
}

pub fn enable_custom_scan() -> bool {
    ENABLE_CUSTOM_SCAN.get()
}

pub fn per_tuple_cost() -> f64 {
    PER_TUPLE_COST.get()
}

pub fn log_create_index_progress() -> bool {
    LOG_CREATE_INDEX_PROGRESS.get()
}

pub fn create_index_parallelism() -> NonZeroUsize {
    adjust_nthreads(CREATE_INDEX_PARALLELISM.get())
}

pub fn create_index_memory_budget() -> usize {
    adjust_budget(CREATE_INDEX_MEMORY_BUDGET.get(), create_index_parallelism())
}

pub fn statement_parallelism() -> NonZeroUsize {
    adjust_nthreads(STATEMENT_PARALLELISM.get())
}

pub fn statement_memory_budget() -> usize {
    adjust_budget(STATEMENT_MEMORY_BUDGET.get(), statement_parallelism())
}

pub fn max_mergeable_segment_size() -> usize {
    MAX_MERGEABLE_SEGMENT_SIZE.get() as usize
}

fn adjust_nthreads(nthreads: i32) -> NonZeroUsize {
    let nthreads = if nthreads <= 0 {
        std::thread::available_parallelism()
            .expect("your computer should have at least one core")
            .get()
    } else {
        nthreads as usize
    };

    unsafe {
        // SAFETY:  we ensured above that nthreads is > 0
        NonZeroUsize::new_unchecked(nthreads)
    }
}

fn adjust_budget(per_thread_budget: i32, parallelism: Parallelism) -> usize {
    // NB:  These limits come from [`tantivy::index_writer::MEMORY_BUDGET_NUM_BYTES_MAX`], which is not publicly exposed
    mod limits {
        // Size of the margin for the `memory_arena`. A segment is closed when the remaining memory
        // in the `memory_arena` goes below MARGIN_IN_BYTES.
        pub const MARGIN_IN_BYTES: usize = 1_000_000;

        // We impose the memory per thread to be at least 15 MB, as the baseline consumption is 12MB.
        pub const MEMORY_BUDGET_NUM_BYTES_MIN: usize = ((MARGIN_IN_BYTES as u32) * 15u32) as usize;
        pub const MEMORY_BUDGET_NUM_BYTES_MAX: usize = u32::MAX as usize - MARGIN_IN_BYTES;
    }

    let per_thread_budget = if per_thread_budget <= 0 {
        // value is unset, so we'll use the maintenance_work_mem, divided between the parallelism value
        let mwm_as_bytes = unsafe {
            // SAFETY:  Postgres sets maintenance_work_mem when it starts up
            pg_sys::maintenance_work_mem as usize * 1024 // convert from kilobytes to bytes
        };

        mwm_as_bytes / parallelism.get()
    } else {
        per_thread_budget as usize * 1024 * 1024 // convert from megabytes to bytes
    };

    // clamp the per_thread_budget to the min/max values
    let per_thread_budget = per_thread_budget.clamp(
        limits::MEMORY_BUDGET_NUM_BYTES_MIN,
        limits::MEMORY_BUDGET_NUM_BYTES_MAX - 1,
    );

    per_thread_budget * parallelism.get()
}
