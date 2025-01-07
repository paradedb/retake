use crate::postgres::customscan::builders::custom_state::CustomScanStateWrapper;
use crate::postgres::customscan::dsm::ParallelQueryCapable;
use crate::postgres::customscan::pdbscan::PdbScan;
use pgrx::pg_sys::{shm_toc, ParallelContext, Size};
use std::os::raw::c_void;
use tantivy::SegmentOrdinal;

// TODO:  get rid of this after community-dev merge
pub mod spinlock {
    use pgrx::pg_sys;
    use std::ptr::addr_of_mut;

    #[derive(Debug)]
    #[repr(transparent)]
    pub struct Spinlock(pub pg_sys::slock_t);

    impl Spinlock {
        #[inline(always)]
        pub fn init(&mut self) {
            unsafe {
                // SAFETY:  `unsafe` due to normal FFI
                pg_sys::SpinLockInit(addr_of_mut!(self.0));
            }
        }

        #[must_use]
        #[inline(always)]
        pub fn acquire(&mut self) -> impl Drop {
            AcquiredSpinLock::new(self)
        }
    }

    #[repr(transparent)]
    pub struct AcquiredSpinLock(*mut pg_sys::slock_t);

    impl AcquiredSpinLock {
        fn new(lock: &mut Spinlock) -> Self {
            unsafe {
                let addr = addr_of_mut!(lock.0);
                pg_sys::SpinLockAcquire(addr);
                Self(addr)
            }
        }
    }

    impl Drop for AcquiredSpinLock {
        #[inline(always)]
        fn drop(&mut self) {
            unsafe {
                pg_sys::SpinLockRelease(self.0);
            }
        }
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct PdbParallelScanState {
    mutex: spinlock::Spinlock,
    segment_count: SegmentOrdinal,
    remaining_segments: SegmentOrdinal,
}

impl ParallelQueryCapable for PdbScan {
    fn estimate_dsm_custom_scan(
        state: &mut CustomScanStateWrapper<Self>,
        pcxt: *mut ParallelContext,
    ) -> Size {
        size_of::<PdbParallelScanState>()
    }

    fn initialize_dsm_custom_scan(
        state: &mut CustomScanStateWrapper<Self>,
        pcxt: *mut ParallelContext,
        coordinate: *mut c_void,
    ) {
        let pscan_state = coordinate.cast::<PdbParallelScanState>();
        assert!(!pscan_state.is_null(), "coordinate is null");

        unsafe {
            (*pscan_state).mutex.init();
            (*pscan_state).segment_count = state
                .custom_state()
                .search_reader
                .as_ref()
                .expect("SearchReader should be set")
                .searcher()
                .segment_readers()
                .len()
                .try_into()
                .expect("should not have more than u32 segments");
            (*pscan_state).remaining_segments = (*pscan_state).segment_count;
            state.custom_state_mut().parallel_state = Some(pscan_state);
        }
    }

    fn reinitialize_dsm_custom_scan(
        state: &mut CustomScanStateWrapper<Self>,
        pcxt: *mut ParallelContext,
        coordinate: *mut c_void,
    ) {
        let pscan_state = coordinate.cast::<PdbParallelScanState>();
        assert!(!pscan_state.is_null(), "coordinate is null");
    }

    fn initialize_worker_custom_scan(
        state: &mut CustomScanStateWrapper<Self>,
        toc: *mut shm_toc,
        coordinate: *mut c_void,
    ) {
        let pscan_state = coordinate.cast::<PdbParallelScanState>();
        assert!(!pscan_state.is_null(), "coordinate is null");

        state.custom_state_mut().parallel_state = Some(pscan_state);
    }
}

pub unsafe fn checkout_segment(pscan_state: *mut PdbParallelScanState) -> Option<SegmentOrdinal> {
    let mutex = (*pscan_state).mutex.acquire();
    if (*pscan_state).remaining_segments > 0 {
        (*pscan_state).remaining_segments -= 1;
        Some((*pscan_state).remaining_segments)
    } else {
        None
    }
}
