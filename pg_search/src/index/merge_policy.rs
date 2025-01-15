use crate::postgres::storage::block::{MergeLockData, MERGE_LOCK};
use crate::postgres::storage::buffer::BufferManager;
use pgrx::pg_sys;
use tantivy::indexer::{MergeCandidate, MergePolicy};
use tantivy::SegmentMeta;

#[derive(Debug, Clone)]
pub enum AllowedMergePolicy {
    None,
    NPlusOne,
}

/// A tantivy [`MergePolicy`] that endeavours to keep a maximum number of segments "N", plus
/// one extra for leftovers.
///
/// It merges the smallest segments, accounting for deleted docs.
#[derive(Debug)]
pub struct NPlusOneMergePolicy {
    // the number of segments we want to maintain on disk
    pub n: usize,

    // the minimum number of segments to merge together
    // if we don't have this many, no merge is performed
    pub min_num_segments: usize,
}

impl MergePolicy for NPlusOneMergePolicy {
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
        let n = self.n;
        let min_num_segments = self.min_num_segments;

        if segments.len() < n + min_num_segments {
            return vec![];
        }

        // collect a list of the segments and sort them largest-to-smallest, by # of alive docs
        let mut segments = segments.iter().collect::<Vec<_>>();
        segments.sort_unstable_by(|a, b| a.num_docs().cmp(&b.num_docs()).reverse());

        let mut candidate = MergeCandidate(vec![]);
        while segments.len() > n {
            let meta = segments.pop().unwrap();
            candidate.0.push(meta.id());
        }

        if candidate.0.len() < 2 {
            // nothing worth merging
            return vec![];
        }

        vec![candidate]
    }
}

// This lock is acquired by inserts if merge_on_insert is true
// Merges should only happen if there is no other merge in progress
// AND the effects of the previous merge are visible
pub unsafe fn acquire_merge_lock(relation_oid: pg_sys::Oid) -> bool {
    if !pg_sys::IsTransactionState() {
        return false;
    }

    let mut bman = BufferManager::new(relation_oid);

    if let Some(mut merge_lock) = bman.get_buffer_conditional(MERGE_LOCK) {
        let mut page = merge_lock.page_mut();
        let metadata = page.contents_mut::<MergeLockData>();
        let last_merge = metadata.last_merge;
        let last_vacuum = metadata.last_vacuum;

        // a merge has never happened before, so it must be safe to merge
        if !pg_sys::TransactionIdIsNormal(last_merge) && !pg_sys::TransactionIdIsNormal(last_vacuum)
        {
            metadata.last_merge = pg_sys::GetCurrentTransactionId();
            return true;
        }

        // allow a merge if the effects of the last merge_on_insert are visible and there is no ongoing vacuum
        // the effects of vacuums are immediately visible and not subject to mvcc, so we don't do mvcc checks on last_vacuum
        let snapshot = pg_sys::GetActiveSnapshot();
        let can_merge = !pg_sys::XidInMVCCSnapshot(last_merge, snapshot)
            && (pg_sys::TransactionIdDidCommit(last_vacuum)
                || pg_sys::TransactionIdDidAbort(last_vacuum));

        if can_merge {
            metadata.last_merge = pg_sys::GetCurrentTransactionId();
        }
        can_merge
    } else {
        false
    }
}

// This lock must be acquired before ambulkdelete calls commit() on the index
// We ask for an exclusive lock because ambulkdelete must delete all dead ctids
pub unsafe fn acquire_delete_lock(relation_oid: pg_sys::Oid) {
    let mut bman = BufferManager::new(relation_oid);

    {
        let mut merge_lock = bman.get_buffer_mut(MERGE_LOCK);
        let mut page = merge_lock.page_mut();
        let metadata = page.contents_mut::<MergeLockData>();
        metadata.last_vacuum = pg_sys::GetCurrentTransactionId();
    }

    // wait for existing merge to finish
    loop {
        pgrx::check_for_interrupts!();

        let merge_lock = bman.get_buffer(MERGE_LOCK);
        let page = merge_lock.page();
        let metadata = page.contents::<MergeLockData>();
        let last_merge = metadata.last_merge;

        if pg_sys::TransactionIdDidCommit(last_merge)
            || pg_sys::TransactionIdDidAbort(last_merge)
            || !pg_sys::TransactionIdIsNormal(last_merge)
        {
            break;
        }
    }
}

pub unsafe fn get_num_segments(relation_oid: pg_sys::Oid) -> u32 {
    let bman = BufferManager::new(relation_oid);
    let buffer = bman.get_buffer(MERGE_LOCK);
    let page = buffer.page();
    let metadata = page.contents::<MergeLockData>();
    metadata.num_segments
}

pub unsafe fn set_num_segments(relation_oid: pg_sys::Oid, num_segments: u32) {
    let mut bman = BufferManager::new(relation_oid);
    let mut buffer = bman.get_buffer_mut(MERGE_LOCK);
    let mut page = buffer.page_mut();
    let metadata = page.contents_mut::<MergeLockData>();
    metadata.num_segments = num_segments;
}
