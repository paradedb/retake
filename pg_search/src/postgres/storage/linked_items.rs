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

use super::block::{
    BM25PageSpecialData, BlockEntry, LinkedList, LinkedListData, MVCCEntry, PgItem,
};
use super::buffer::{BufferManager, BufferMut};
use super::utils::vacuum_get_freeze_limit;
use anyhow::Result;
use pgrx::pg_sys;
use pgrx::pg_sys::BlockNumber;
use std::fmt::Debug;
// ---------------------------------------------------------------
// Linked list implementation over block storage,
// where each node in the list is a pg_sys::Item
// ---------------------------------------------------------------

// +-------------------------------------------------------------+
// |                       Header Buffer                         |
// +-------------------------------------------------------------+
// | LinkedListData                                              |
// +-------------------------------------------------------------+
// | LP_SPECIAL                                                  |
// | [next_blockno: BlockNumber, xmax: TransactionId]            |
// +-------------------------------------------------------------+

// +-------------------------------------------------------------+
// |                        Start Buffer                         |
// +-------------------------------------------------------------+
// | [Item] [Item] [Item] ...                                    |
// +-------------------------------------------------------------+
// | LP_SPECIAL                                                  |
// | [next_blockno: BlockNumber, xmax: TransactionId]            |
// +-------------------------------------------------------------+

// ... repeat until the last block

// +-------------------------------------------------------------+
// |                        Last Buffer                          |
// +-------------------------------------------------------------+
// | [Item] [Item] [Item] ...                                    |
// +-------------------------------------------------------------+
// | LP_SPECIAL                                                  |
// | [next_blockno: BlockNumber, xmax: TransactionId]            |
// +-------------------------------------------------------------+

pub struct LinkedItemList<T: From<PgItem> + Into<PgItem> + Debug + Clone + MVCCEntry + BlockEntry> {
    relation_oid: pg_sys::Oid,
    pub header_blockno: pg_sys::BlockNumber,
    bman: BufferManager,
    _marker: std::marker::PhantomData<T>,
}

impl<T: From<PgItem> + Into<PgItem> + Debug + Clone + MVCCEntry + BlockEntry> LinkedList
    for LinkedItemList<T>
{
    fn get_header_blockno(&self) -> pg_sys::BlockNumber {
        self.header_blockno
    }

    fn block_for_ord(&self, ord: usize) -> Option<BlockNumber> {
        unimplemented!(
            "block_for_ord is not implemented for LinkedItemList.  caller requested ord: {ord}"
        )
    }

    unsafe fn get_linked_list_data(&self) -> LinkedListData {
        let header_buffer = self.bman.get_buffer(self.get_header_blockno());
        let page = header_buffer.page();
        page.contents::<LinkedListData>()
    }
}

impl<T: From<PgItem> + Into<PgItem> + Debug + Clone + MVCCEntry + BlockEntry> LinkedItemList<T> {
    pub fn open(relation_oid: pg_sys::Oid, header_blockno: pg_sys::BlockNumber) -> Self {
        Self {
            relation_oid,
            header_blockno,
            bman: BufferManager::new(relation_oid),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn bman(&self) -> &BufferManager {
        &self.bman
    }

    pub fn bman_mut(&mut self) -> &mut BufferManager {
        &mut self.bman
    }

    pub unsafe fn create(relation_oid: pg_sys::Oid) -> Self {
        let mut bman = BufferManager::new(relation_oid);

        let mut header_buffer = bman.new_buffer();
        let header_blockno = header_buffer.number();
        let mut start_buffer = bman.new_buffer();
        let start_blockno = start_buffer.number();

        let mut header_page = header_buffer.init_page();
        start_buffer.init_page();

        let metadata = header_page.contents_mut::<LinkedListData>();
        metadata.start_blockno = start_blockno;
        metadata.last_blockno = start_blockno;
        metadata.npages = 0;

        Self {
            relation_oid,
            header_blockno,
            bman,
            _marker: std::marker::PhantomData,
        }
    }

    /// Return a Vec of all the items in this linked list
    pub unsafe fn list(&self) -> Vec<T> {
        let mut items = vec![];
        let mut blockno = self.get_start_blockno();

        while blockno != pg_sys::InvalidBlockNumber {
            let buffer = self.bman.get_buffer(blockno);
            let page = buffer.page();
            let mut offsetno = pg_sys::FirstOffsetNumber;
            let max_offset = page.max_offset_number();
            while offsetno <= max_offset {
                if let Some((deserialized, _)) = page.read_item::<T>(offsetno) {
                    items.push(deserialized);
                }
                offsetno += 1;
            }
            blockno = page.next_blockno();
        }

        items
    }

    pub unsafe fn garbage_collect(&mut self, strategy: pg_sys::BufferAccessStrategy) -> Result<()> {
        // Delete all items that are definitely dead
        let snapshot = pg_sys::GetActiveSnapshot();
        // let heap_oid = pg_sys::IndexGetRelation(self.relation_oid, false);
        // let heap_relation = pg_sys::RelationIdGetRelation(heap_oid);
        let freeze_limit = vacuum_get_freeze_limit(std::ptr::null_mut());
        let start_blockno = self.get_start_blockno();
        let mut blockno = start_blockno;
        let mut last_filled_blockno = start_blockno;

        while blockno != pg_sys::InvalidBlockNumber {
            let mut buffer = self.bman.get_buffer_for_cleanup(blockno, strategy);
            let mut page = buffer.page_mut();
            let mut offsetno = pg_sys::FirstOffsetNumber;
            let max_offset = page.max_offset_number();
            let mut delete_offsets = vec![];
            let mut pages_to_recycle = vec![];

            while offsetno <= max_offset {
                if let Some((entry, _)) = page.read_item::<T>(offsetno) {
                    if entry.recyclable(snapshot, std::ptr::null_mut()) {
                        delete_offsets.push(offsetno);
                        pages_to_recycle.extend(entry.get_associated_blocks(self.relation_oid));
                    } else {
                        let xmin_needs_freeze = entry.xmin_needs_freeze(freeze_limit);
                        let xmax_needs_freeze = entry.xmax_needs_freeze(freeze_limit);

                        if xmin_needs_freeze || xmax_needs_freeze {
                            let frozen_entry =
                                entry.into_frozen(xmin_needs_freeze, xmax_needs_freeze);
                            let PgItem(item, size) = frozen_entry.clone().into();
                            let did_replace = page.replace_item(offsetno, item, size);
                            assert!(did_replace);
                        }
                    }
                }
                offsetno += 1;
            }

            if !delete_offsets.is_empty() {
                page.delete_items(&mut delete_offsets);
            }

            let new_max_offset = page.max_offset_number();
            let current_blockno = blockno;
            blockno = page.next_blockno();

            // Compaction step: If the page is entirely empty, mark as deleted
            // Adjust the pointer from the last known non-empty node to point to the next non-empty node
            if new_max_offset == pg_sys::InvalidOffsetNumber && current_blockno != start_blockno {
                page.mark_deleted();

                // drop the buffer were holding onto as we might acquire an exclusive lock on another
                // one below and we don't want concurrent backends that are otherwise racing through
                // this linked list to end up causing a lock inversion where they've locked the page
                // we want while we have this page locked
                drop(buffer);

                // We've reached the end of the list, which means the last filled block is now the
                // last entry in the list
                if blockno == pg_sys::InvalidBlockNumber {
                    let mut last_filled_buffer = self.bman.get_buffer_mut(last_filled_blockno);
                    let mut last_filled_page = last_filled_buffer.page_mut();
                    let special = last_filled_page.special_mut::<BM25PageSpecialData>();
                    special.next_blockno = pg_sys::InvalidBlockNumber;
                }
            } else if new_max_offset != pg_sys::InvalidOffsetNumber
                && current_blockno != start_blockno
            {
                // drop the buffer were holding onto as we might acquire an exclusive lock on another
                // one below and we don't want concurrent backends that are otherwise racing through
                // this linked list to end up causing a lock inversion where they've locked the page
                // we want while we have this page locked
                drop(buffer);

                let mut last_filled_buffer = self.bman.get_buffer_mut(last_filled_blockno);
                let mut last_filled_page = last_filled_buffer.page_mut();
                if last_filled_page.next_blockno() != current_blockno {
                    let special = last_filled_page.special_mut::<BM25PageSpecialData>();
                    special.next_blockno = current_blockno;
                }
                last_filled_blockno = current_blockno;
            }

            let mut vacuum_fsm = false;
            for blockno in pages_to_recycle {
                let buffer = self.bman.get_buffer(blockno);
                let page = buffer.page();
                if page.is_recyclable(std::ptr::null_mut()) {
                    self.bman.record_free_index_page(buffer);
                    vacuum_fsm = true;
                }
            }

            if vacuum_fsm {
                let index = pg_sys::RelationIdGetRelation(self.relation_oid);
                pg_sys::IndexFreeSpaceMapVacuum(index);
                pg_sys::RelationClose(index);
            }
        }

        // pg_sys::RelationClose(heap_relation);
        Ok(())
    }

    pub unsafe fn add_items(&mut self, items: Vec<T>, buffer: Option<BufferMut>) -> Result<()> {
        let need_hold = buffer.is_some();
        let mut buffer =
            buffer.unwrap_or_else(|| self.bman.get_buffer_mut(self.get_start_blockno()));

        // this will get set to the `buffer` argument above and remain open (ie, exclusive locked)
        // until we're done adding all the items, so long as the original `buffer` argument was
        // Some(BufferMut)
        let mut hold_open = None;

        for item in items {
            let PgItem(pg_item, size) = item.into();

            'append_loop: loop {
                let mut page = buffer.page_mut();
                let offsetno = page.append_item(pg_item, size, 0);
                if offsetno != pg_sys::InvalidOffsetNumber {
                    // it added to this block
                    break 'append_loop;
                } else if page.next_blockno() != pg_sys::InvalidBlockNumber {
                    // go to the next block
                    let next_blockno = page.next_blockno();
                    if need_hold && hold_open.is_none() {
                        hold_open = Some(buffer);
                    }
                    buffer = self.bman.get_buffer_mut(next_blockno);
                } else {
                    // need to create new block and link it to this one
                    let mut new_page = self.bman.new_buffer();
                    let new_blockno = new_page.number();
                    new_page.init_page();

                    let special = page.special_mut::<BM25PageSpecialData>();
                    special.next_blockno = new_blockno;

                    // Update the header to point to the new last page
                    let mut header_buffer = self.bman.get_buffer_mut(self.header_blockno);
                    let mut page = header_buffer.page_mut();
                    let metadata = page.contents_mut::<LinkedListData>();
                    metadata.last_blockno = new_blockno;
                    metadata.npages += 1;

                    if need_hold && hold_open.is_none() {
                        hold_open = Some(buffer);
                    }
                    buffer = new_page;
                }
            }
        }

        Ok(())
    }

    pub unsafe fn lookup<Cmp: Fn(&T) -> bool>(&self, cmp: Cmp) -> Result<T> {
        self.lookup_ex(cmp).map(|(t, _, _)| t)
    }

    pub unsafe fn lookup_ex<Cmp: Fn(&T) -> bool>(
        &self,
        cmp: Cmp,
    ) -> Result<(T, pg_sys::BlockNumber, pg_sys::OffsetNumber)> {
        let mut blockno = self.get_start_blockno();

        while blockno != pg_sys::InvalidBlockNumber {
            let buffer = self.bman.get_buffer(blockno);
            let page = buffer.page();
            let mut offsetno = pg_sys::FirstOffsetNumber;
            let max_offset = page.max_offset_number();

            while offsetno <= max_offset {
                if let Some((deserialized, _)) = page.read_item::<T>(offsetno) {
                    if cmp(&deserialized) {
                        return Ok((deserialized, blockno, offsetno));
                    }
                }
                offsetno += 1;
            }

            blockno = page.next_blockno();
        }

        // if we get here, we didn't find what we were looking for
        // but we should have -- how else could we have asked for something from the list?
        Err(anyhow::anyhow!(format!(
            "transaction {} failed to find the desired entry",
            pg_sys::GetCurrentTransactionId()
        )))
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use pgrx::prelude::*;
    use std::collections::HashSet;
    use tantivy::index::SegmentId;
    use uuid::Uuid;

    use crate::postgres::storage::block::SegmentMetaEntry;

    fn random_segment_id() -> SegmentId {
        SegmentId::from_uuid_string(&Uuid::new_v4().to_string()).unwrap()
    }

    fn linked_list_block_numbers(
        list: &LinkedItemList<SegmentMetaEntry>,
    ) -> HashSet<pg_sys::BlockNumber> {
        let mut blockno = list.get_start_blockno();
        let mut block_numbers = HashSet::new();

        while blockno != pg_sys::InvalidBlockNumber {
            let buffer = list.bman().get_buffer(blockno);
            let page = buffer.page();
            blockno = page.next_blockno();
            block_numbers.insert(blockno);
        }

        block_numbers
    }

    #[pg_test]
    unsafe fn test_linked_items_garbage_collect_single_page() {
        Spi::run("CREATE TABLE t (id SERIAL, data TEXT);").unwrap();
        Spi::run("CREATE INDEX t_idx ON t USING bm25(id, data) WITH (key_field = 'id')").unwrap();
        let relation_oid: pg_sys::Oid =
            Spi::get_one("SELECT oid FROM pg_class WHERE relname = 't_idx' AND relkind = 'i';")
                .expect("spi should succeed")
                .unwrap();

        let strategy = pg_sys::GetAccessStrategy(pg_sys::BufferAccessStrategyType::BAS_VACUUM);
        let snapshot = pg_sys::GetActiveSnapshot();
        let delete_xid = {
            #[cfg(feature = "pg13")]
            {
                pg_sys::RecentGlobalXmin - 1
            }
            #[cfg(not(feature = "pg13"))]
            {
                (*snapshot).xmin - 1
            }
        };

        let mut list = LinkedItemList::<SegmentMetaEntry>::create(relation_oid);
        let entries_to_delete = vec![SegmentMetaEntry {
            segment_id: random_segment_id(),
            xmin: delete_xid,
            xmax: delete_xid,
            ..Default::default()
        }];
        let entries_to_keep = vec![SegmentMetaEntry {
            segment_id: random_segment_id(),
            xmin: (*snapshot).xmin - 1,
            xmax: pg_sys::InvalidTransactionId,
            ..Default::default()
        }];

        list.add_items(entries_to_delete.clone(), None).unwrap();
        list.add_items(entries_to_keep.clone(), None).unwrap();
        list.garbage_collect(strategy).unwrap();

        assert!(list
            .lookup(|entry| entry.segment_id == entries_to_delete[0].segment_id)
            .is_err());
        assert!(list
            .lookup(|entry| entry.segment_id == entries_to_keep[0].segment_id)
            .is_ok());
    }

    #[pg_test]
    unsafe fn test_linked_items_garbage_collect_multiple_pages() {
        Spi::run("CREATE TABLE t (id SERIAL, data TEXT);").unwrap();
        Spi::run("CREATE INDEX t_idx ON t USING bm25(id, data) WITH (key_field = 'id')").unwrap();
        let relation_oid: pg_sys::Oid =
            Spi::get_one("SELECT oid FROM pg_class WHERE relname = 't_idx' AND relkind = 'i';")
                .expect("spi should succeed")
                .unwrap();

        let strategy = pg_sys::GetAccessStrategy(pg_sys::BufferAccessStrategyType::BAS_VACUUM);
        let snapshot = pg_sys::GetActiveSnapshot();
        let deleted_xid = {
            #[cfg(feature = "pg13")]
            {
                pg_sys::RecentGlobalXmin - 1
            }
            #[cfg(not(feature = "pg13"))]
            {
                (*snapshot).xmin - 1
            }
        };
        let not_deleted_xid = pg_sys::InvalidTransactionId;
        let xmin = (*snapshot).xmin - 1;

        // Add 2000 entries, delete every 10th entry
        {
            let mut list = LinkedItemList::<SegmentMetaEntry>::create(relation_oid);
            let entries = (1..2000)
                .map(|i| SegmentMetaEntry {
                    segment_id: random_segment_id(),
                    xmin,
                    xmax: if i % 10 == 0 {
                        deleted_xid
                    } else {
                        not_deleted_xid
                    },
                    ..Default::default()
                })
                .collect::<Vec<_>>();

            list.add_items(entries.clone(), None).unwrap();
            list.garbage_collect(strategy).unwrap();

            for entry in entries {
                if entry.xmax == not_deleted_xid {
                    assert!(list.lookup(|el| el.segment_id == entry.segment_id).is_ok());
                } else {
                    assert!(list.lookup(|el| el.segment_id == entry.segment_id).is_err());
                }
            }
        }
        // First n pages are full, next m pages need to be compacted, next n are full
        {
            let mut list = LinkedItemList::<SegmentMetaEntry>::create(relation_oid);
            let entries_1 = (1..500)
                .map(|_| SegmentMetaEntry {
                    segment_id: random_segment_id(),
                    xmin,
                    xmax: not_deleted_xid,
                    ..Default::default()
                })
                .collect::<Vec<_>>();
            list.add_items(entries_1.clone(), None).unwrap();

            let entries_2 = (1..1000)
                .map(|_| SegmentMetaEntry {
                    segment_id: random_segment_id(),
                    xmin,
                    xmax: deleted_xid,
                    ..Default::default()
                })
                .collect::<Vec<_>>();
            list.add_items(entries_2.clone(), None).unwrap();

            let entries_3 = (1..500)
                .map(|_| SegmentMetaEntry {
                    segment_id: random_segment_id(),
                    xmin,
                    xmax: not_deleted_xid,
                    ..Default::default()
                })
                .collect::<Vec<_>>();

            list.add_items(entries_3.clone(), None).unwrap();

            let pre_gc_blocks = linked_list_block_numbers(&list);
            list.garbage_collect(strategy).unwrap();

            for entries in [entries_1, entries_2, entries_3] {
                for entry in entries {
                    if entry.xmax == not_deleted_xid {
                        assert!(list.lookup(|el| el.segment_id == entry.segment_id).is_ok());
                    } else {
                        assert!(list.lookup(|el| el.segment_id == entry.segment_id).is_err());
                    }
                }
            }

            // Assert that compaction produced a smaller list with no new blocks
            let post_gc_blocks = linked_list_block_numbers(&list);
            assert!(pre_gc_blocks.len() > post_gc_blocks.len());
            assert!(post_gc_blocks.is_subset(&pre_gc_blocks));
        }
    }
}
