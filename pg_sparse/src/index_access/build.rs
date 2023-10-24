use hnswlib::Index;
use pgrx::*;
use std::panic::{self, AssertUnwindSafe};

use crate::index_access::options::SparseOptions;
use crate::sparse_index::index::create_index;
use crate::sparse_index::sparse::Sparse;

struct BuildState<'a> {
    count: usize,
    sparse_index: &'a mut Index,
    memcxt: PgMemoryContexts,
}

impl<'a> BuildState<'a> {
    fn new(sparse_index: &'a mut Index) -> Self {
        BuildState {
            sparse_index,
            count: 0,
            memcxt: PgMemoryContexts::new("HNSW build context"),
        }
    }
}

#[pg_guard]
pub extern "C" fn ambuild(
    heaprel: pg_sys::Relation,
    index: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    // Create Index
    let mut sparse_index = create_index(index);

    let heap_relation = unsafe { PgRelation::from_pg(heaprel) };
    let index_relation = unsafe { PgRelation::from_pg(index) };
    let ntuples = do_heap_scan(
        index_info,
        &heap_relation,
        &index_relation,
        &mut sparse_index,
    );

    let mut result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    result.heap_tuples = ntuples as f64;
    result.index_tuples = ntuples as f64;

    result.into_pg()
}

#[pg_guard]
pub extern "C" fn ambuildempty(_index_relation: pg_sys::Relation) {}

fn do_heap_scan<'a>(
    index_info: *mut pg_sys::IndexInfo,
    heap_relation: &'a PgRelation,
    index_relation: &'a PgRelation,
    sparse_index: &mut Index,
) -> usize {
    let mut state = BuildState::new(sparse_index);
    let _ = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        pg_sys::IndexBuildHeapScan(
            heap_relation.as_ptr(),
            index_relation.as_ptr(),
            index_info,
            Some(build_callback),
            &mut state,
        );
    }));
    state.count
}

#[cfg(any(feature = "pg10", feature = "pg11", feature = "pg12"))]
#[pg_guard]
unsafe extern "C" fn build_callback(
    index: pg_sys::Relation,
    htup: pg_sys::HeapTuple,
    values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    let htup = htup.as_ref().unwrap();

    build_callback_internal(htup.t_self, values, state, index);
}

#[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15"))]
#[pg_guard]
unsafe extern "C" fn build_callback(
    index: pg_sys::Relation,
    ctid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    build_callback_internal(*ctid, values, state, index);
}

#[inline(always)]
unsafe extern "C" fn build_callback_internal(
    ctid: pg_sys::ItemPointerData,
    values: *mut pg_sys::Datum,
    state: *mut std::os::raw::c_void,
    index: pg_sys::Relation,
) {
    check_for_interrupts!();

    let index_relation_ref = unsafe { PgRelation::from_pg(index) };
    let state = (state as *mut BuildState).as_mut().unwrap();
    let mut old_context = state.memcxt.set_as_current();

    let values = std::slice::from_raw_parts(values, 1);
    let sparse_vector: Option<Sparse> = FromDatum::from_datum(values[0], false);

    if let Some(sparse_vector) = sparse_vector {
        state
            .sparse_index
            .add_sparse_vector(sparse_vector.entries, item_pointer_to_u64(ctid) as usize);
    }

    old_context.set_as_current();
    state.memcxt.reset();
}
