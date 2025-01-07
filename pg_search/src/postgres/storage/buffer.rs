use crate::postgres::storage::block::{BM25PageSpecialData, PgItem};
use crate::postgres::storage::utils::{BM25BufferCache, BM25Page};
use crate::postgres::NeedWal;
use pgrx::pg_sys;
use std::ptr::NonNull;
use std::sync::Arc;

#[derive(Debug, Copy, Clone, Default)]
#[repr(i32)]
enum XlogFlag {
    #[default]
    ExistingBuffer = 0,
    NewBuffer = pg_sys::GENERIC_XLOG_FULL_IMAGE as i32,
}
#[derive(Copy, Clone, Default)]
enum XlogStyle {
    #[default]
    Unlogged,
    #[allow(dead_code)]
    Logged(NonNull<pg_sys::GenericXLogState>, XlogFlag),
}

impl XlogStyle {
    fn get_page(&self, buffer: pg_sys::Buffer) -> pg_sys::Page {
        unsafe { pg_sys::BufferGetPage(buffer) }
    }
}

pub struct Buffer {
    pg_buffer: pg_sys::Buffer,
}

impl Drop for Buffer {
    fn drop(&mut self) {
        unsafe {
            if pg_sys::IsTransactionState() {
                pg_sys::UnlockReleaseBuffer(self.pg_buffer);
            }
        }
    }
}

impl Buffer {
    pub fn page(&self) -> Page {
        let pg_page = unsafe { pg_sys::BufferGetPage(self.pg_buffer) };
        Page {
            pg_page,
            _buffer: self,
        }
    }

    pub fn page_contents<T: Copy>(&self) -> T {
        self.page().contents()
    }

    pub fn number(&self) -> pg_sys::BlockNumber {
        unsafe { pg_sys::BufferGetBlockNumber(self.pg_buffer) }
    }

    pub fn page_size(&self) -> pg_sys::Size {
        unsafe { pg_sys::BufferGetPageSize(self.pg_buffer) }
    }
}

pub struct BufferMut {
    style: XlogStyle,
    dirty: bool,
    inner: Buffer,
}

impl Drop for BufferMut {
    fn drop(&mut self) {
        unsafe {
            if pg_sys::IsTransactionState() && self.dirty {
                pg_sys::MarkBufferDirty(self.inner.pg_buffer);
            }
        }
    }
}

impl BufferMut {
    pub fn init_page(&mut self) -> PageMut {
        let page_size = self.page_size();
        let page = self.page_mut();
        page.buffer.dirty = true;
        unsafe {
            pg_sys::PageInit(page.pg_page, page_size, size_of::<BM25PageSpecialData>());

            let special = pg_sys::PageGetSpecialPointer(page.pg_page) as *mut BM25PageSpecialData;
            (*special).next_blockno = pg_sys::InvalidBlockNumber;
            (*special).xmax = pg_sys::InvalidTransactionId;
        }
        page
    }

    #[allow(dead_code)]
    pub fn page(&self) -> Page {
        unsafe {
            Page {
                pg_page: pg_sys::BufferGetPage(self.inner.pg_buffer),
                _buffer: &self.inner,
            }
        }
    }

    pub fn page_mut(&mut self) -> PageMut {
        let pg_page = self.style.get_page(self.inner.pg_buffer);
        PageMut {
            buffer: self,
            pg_page,
        }
    }

    pub fn number(&self) -> pg_sys::BlockNumber {
        self.inner.number()
    }

    pub fn page_size(&self) -> pg_sys::Size {
        self.inner.page_size()
    }
}

pub struct Page<'a> {
    pg_page: pg_sys::Page,

    // we never use this directly, but we hold onto it so that its Drop impl
    // won't run while we're live
    _buffer: &'a Buffer,
}

impl Page<'_> {
    #[allow(dead_code)]
    pub fn free_space(&self) -> usize {
        unsafe { pg_sys::PageGetFreeSpace(self.pg_page) }
    }

    pub fn is_empty(&self) -> bool {
        unsafe { pg_sys::PageIsEmpty(self.pg_page) }
    }

    pub fn max_offset_number(&self) -> pg_sys::OffsetNumber {
        unsafe { pg_sys::PageGetMaxOffsetNumber(self.pg_page) }
    }

    pub fn read_item<T: From<PgItem>>(
        &self,
        offno: pg_sys::OffsetNumber,
    ) -> Option<(T, pg_sys::Size)> {
        unsafe { self.pg_page.read_item(offno) }
    }

    pub fn header(&self) -> &pg_sys::PageHeaderData {
        unsafe { &*(self.pg_page as *const pg_sys::PageHeaderData) }
    }

    pub fn special<T>(&self) -> &T {
        unsafe { &*(pg_sys::PageGetSpecialPointer(self.pg_page) as *const T) }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            let header_size = std::mem::offset_of!(pg_sys::PageHeaderData, pd_linp);
            let slice_len = self.header().pd_lower as usize - header_size;
            std::slice::from_raw_parts((self.pg_page as *const u8).add(header_size), slice_len)
        }
    }

    pub fn as_slice_range(&self, offset: usize, len: usize) -> &[u8] {
        unsafe {
            let header_size = std::mem::offset_of!(pg_sys::PageHeaderData, pd_linp);
            std::slice::from_raw_parts((self.pg_page as *const u8).add(header_size + offset), len)
        }
    }

    pub fn contents<T: Copy>(&self) -> T {
        unsafe { (pg_sys::PageGetContents(self.pg_page) as *const T).read_unaligned() }
    }

    pub fn is_recyclable(&self, heaprel: pg_sys::Relation) -> bool {
        unsafe { self.pg_page.recyclable(heaprel) }
    }

    pub fn next_blockno(&self) -> pg_sys::BlockNumber {
        unsafe {
            let special = pg_sys::PageGetSpecialPointer(self.pg_page) as *mut BM25PageSpecialData;
            (*special).next_blockno
        }
    }
}

pub struct PageMut<'a> {
    buffer: &'a mut BufferMut,
    pg_page: pg_sys::Page,
}

impl PageMut<'_> {
    pub fn mark_deleted(mut self) {
        unsafe {
            self.special_mut::<BM25PageSpecialData>().xmax =
                pg_sys::ReadNextFullTransactionId().value as pg_sys::TransactionId;
        }
        self.buffer.dirty = true;
    }

    pub fn max_offset_number(&self) -> pg_sys::OffsetNumber {
        unsafe { pg_sys::PageGetMaxOffsetNumber(self.pg_page) }
    }

    pub fn read_item<T: From<PgItem>>(
        &self,
        offno: pg_sys::OffsetNumber,
    ) -> Option<(T, pg_sys::Size)> {
        unsafe { self.pg_page.read_item(offno) }
    }

    pub fn find_item<T: From<PgItem>, F: Fn(T) -> bool>(
        &self,
        cmp: F,
    ) -> Option<pg_sys::OffsetNumber> {
        let max = self.max_offset_number();
        for offno in pg_sys::FirstOffsetNumber as pg_sys::OffsetNumber..=max {
            let (item, _) = self.read_item::<T>(offno)?;
            if cmp(item) {
                return Some(offno);
            }
        }
        None
    }

    #[must_use]
    #[track_caller]
    pub fn append_item(
        &mut self,
        item: pg_sys::Item,
        size: pg_sys::Size,
        flags: i32,
    ) -> pg_sys::OffsetNumber {
        let offno = unsafe {
            pg_sys::PageAddItemExtended(
                self.pg_page,
                item,
                size,
                pg_sys::InvalidOffsetNumber,
                flags,
            )
        };
        if offno != pg_sys::InvalidOffsetNumber {
            self.buffer.dirty = true;
        }
        offno
    }

    #[must_use]
    pub fn replace_item(
        &mut self,
        offno: pg_sys::OffsetNumber,
        item: pg_sys::Item,
        size: pg_sys::Size,
    ) -> bool {
        assert!(offno != pg_sys::InvalidOffsetNumber);
        let did_replace =
            unsafe { pg_sys::PageIndexTupleOverwrite(self.pg_page, offno, item, size) };
        if did_replace {
            self.buffer.dirty = true;
        }
        did_replace
    }

    pub fn delete_items(&mut self, item_offsets: &mut [pg_sys::OffsetNumber]) {
        // assert the list of item offsets is sorted.  Thanks, stack overflow -- it's too late for me to come up with this on my own
        debug_assert!(item_offsets.windows(2).all(|w| w[0] <= w[1]));
        unsafe {
            pg_sys::PageIndexMultiDelete(
                self.pg_page,
                item_offsets.as_mut_ptr(),
                item_offsets.len() as i32,
            );
            self.buffer.dirty = true;
        }
    }

    pub fn delete_item(&mut self, offno: pg_sys::OffsetNumber) {
        unsafe {
            pg_sys::PageIndexTupleDelete(self.pg_page, offno);
        }
        self.buffer.dirty = true;
    }

    pub fn header(&self) -> &pg_sys::PageHeaderData {
        unsafe { &*(self.pg_page as *const pg_sys::PageHeaderData) }
    }

    pub fn header_mut(&mut self) -> &mut pg_sys::PageHeaderData {
        let header = unsafe { &mut *(self.pg_page as *mut pg_sys::PageHeaderData) };
        self.buffer.dirty = true;
        header
    }

    pub fn special<T>(&self) -> &T {
        unsafe { &*(pg_sys::PageGetSpecialPointer(self.pg_page) as *const T) }
    }

    pub fn special_mut<T>(&mut self) -> &mut T {
        let special = unsafe { &mut *(pg_sys::PageGetSpecialPointer(self.pg_page) as *mut T) };
        self.buffer.dirty = true;
        special
    }

    pub fn free_space_slice_mut(&mut self, len: usize) -> &mut [u8] {
        let slice = unsafe {
            std::slice::from_raw_parts_mut(
                (self.pg_page as *mut u8).add(self.header_mut().pd_lower as usize),
                len,
            )
        };
        self.buffer.dirty = true;
        slice
    }

    pub fn contents_mut<T>(&mut self) -> &mut T {
        let contents = unsafe {
            let contents = pg_sys::PageGetContents(self.pg_page) as *mut T;

            // adjust pd_lower at the same time
            let header = self.pg_page as *mut pg_sys::PageHeaderData;
            (*header).pd_lower = (contents.add(1) as usize - header as usize)
                .try_into()
                .expect("pd_lower overflowed");

            &mut *contents
        };
        self.buffer.dirty = true;
        contents
    }

    pub fn next_blockno(&self) -> pg_sys::BlockNumber {
        unsafe {
            let special = pg_sys::PageGetSpecialPointer(self.pg_page) as *mut BM25PageSpecialData;
            (*special).next_blockno
        }
    }
}

pub trait PageHeaderMethods {
    fn free_space(&self) -> usize;
}

impl PageHeaderMethods for pg_sys::PageHeaderData {
    fn free_space(&self) -> usize {
        self.pd_upper as usize - self.pd_lower as usize
    }
}

#[derive(Clone, Debug)]
pub struct BufferManager {
    logged: bool,
    bcache: Arc<BM25BufferCache>,
}

impl BufferManager {
    pub fn new(indexrelid: pg_sys::Oid, need_wal: NeedWal) -> Self {
        Self {
            logged: need_wal,
            bcache: BM25BufferCache::open(indexrelid),
        }
    }

    pub fn bm25cache(&self) -> &BM25BufferCache {
        &self.bcache
    }

    fn style(&self, flag: XlogFlag) -> XlogStyle {
        if self.logged {
            unsafe { XlogStyle::Logged(NonNull::new_unchecked(self.bcache.start_xlog()), flag) }
        } else {
            XlogStyle::Unlogged
        }
    }

    #[must_use]
    pub fn new_buffer(&mut self) -> BufferMut {
        unsafe {
            BufferMut {
                style: self.style(XlogFlag::NewBuffer),
                dirty: false,
                inner: Buffer {
                    pg_buffer: self.bcache.new_buffer(),
                },
            }
        }
    }

    pub fn get_buffer(&self, blockno: pg_sys::BlockNumber) -> Buffer {
        unsafe {
            Buffer {
                pg_buffer: self
                    .bcache
                    .get_buffer(blockno, Some(pg_sys::BUFFER_LOCK_SHARE)),
            }
        }
    }

    pub fn get_buffer_mut(&mut self, blockno: pg_sys::BlockNumber) -> BufferMut {
        unsafe {
            BufferMut {
                style: self.style(XlogFlag::ExistingBuffer),
                dirty: false,
                inner: Buffer {
                    pg_buffer: self
                        .bcache
                        .get_buffer(blockno, Some(pg_sys::BUFFER_LOCK_EXCLUSIVE)),
                },
            }
        }
    }

    pub fn get_buffer_conditional(&mut self, blockno: pg_sys::BlockNumber) -> Option<BufferMut> {
        unsafe {
            let pg_buffer = self.bcache.get_buffer(blockno, None);
            if pg_sys::ConditionalLockBuffer(pg_buffer) {
                Some(BufferMut {
                    style: self.style(XlogFlag::ExistingBuffer),
                    dirty: false,
                    inner: Buffer { pg_buffer },
                })
            } else {
                pg_sys::ReleaseBuffer(pg_buffer);
                None
            }
        }
    }

    pub fn get_buffer_for_cleanup(
        &mut self,
        blockno: pg_sys::BlockNumber,
        strategy: pg_sys::BufferAccessStrategy,
    ) -> BufferMut {
        unsafe {
            let buffer = self
                .bcache
                .get_buffer_with_strategy(blockno, strategy, None);
            pg_sys::LockBufferForCleanup(buffer);
            BufferMut {
                style: self.style(XlogFlag::ExistingBuffer),
                dirty: false,
                inner: Buffer { pg_buffer: buffer },
            }
        }
    }

    pub fn page_is_empty(&self, blockno: pg_sys::BlockNumber) -> bool {
        self.get_buffer(blockno).page().is_empty()
    }

    pub fn record_free_index_page(&mut self, buffer: Buffer) {
        unsafe {
            self.bcache.record_free_index_page(buffer.number());
        }
    }
}
