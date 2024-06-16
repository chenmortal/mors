use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use mors_common::mmap::MmapFileBuilder;
use mors_common::page_size;
use mors_encrypt::registry::Kms;
use mors_traits::file_id::FileId;
use mors_traits::memtable::Memtable;
use mors_traits::skip_list::SkipList;
use mors_traits::ts::{KeyTsBorrow, TxnTs};
use mors_wal::LogFile;

use crate::{DEFAULT_DIR, MorsMemtableId};
use crate::error::MorsMemtableError;
use crate::Result;

pub struct MorsMemtable<T: SkipList> {
    pub(crate) skip_list: T,
    pub(crate) wal: LogFile<MorsMemtableId>,
    pub(crate) max_version: TxnTs,
    pub(crate) buf: Vec<u8>,
    pub(crate) memtable_size: usize,
    pub(crate) read_only: bool,
}

pub struct MorsMemtableBuilder<T: SkipList> {
    dir: PathBuf,
    read_only: bool,
    memtable_size: usize,
    num_memtables: usize,
    next_fid: Arc<AtomicU32>,
    t: PhantomData<T>,
}
impl<T: SkipList> Default for MorsMemtableBuilder<T> {
    fn default() -> Self {
        Self {
            dir: PathBuf::from(DEFAULT_DIR),
            read_only: false,
            memtable_size: 64 << 20,
            num_memtables: 5,
            next_fid: Default::default(),
            t: Default::default(),
        }
    }
}
// opt.maxBatchSize = (15 * opt.MemTableSize) / 100
// opt.maxBatchCount = opt.maxBatchSize / int64(skl.MaxNodeSize)
impl<T: SkipList> MorsMemtableBuilder<T>
where
    MorsMemtableError: From<<T as SkipList>::ErrorType>,
{
    fn arena_size(&self) -> usize {
        self.memtable_size + 2 * self.max_batch_size()
    }
    fn max_batch_size(&self) -> usize {
        (15 * self.memtable_size) / 100
    }
    fn max_batch_count(&self) -> usize {
        self.max_batch_size() / T::MAX_NODE_SIZE
    }
    pub fn open(
        &self,
        mmap_builder: MmapFileBuilder,
        kms: Kms,
        fid: MorsMemtableId,
    ) -> Result<MorsMemtable<T>> {
        let mem_path = fid.join_dir(self.dir.clone());
        let skip_list = T::new(self.arena_size(), KeyTsBorrow::cmp)?;

        let wal = LogFile::open(
            fid,
            mem_path,
            2 * self.memtable_size as u64,
            mmap_builder,
            kms,
        )?;
        let mut memtable = MorsMemtable {
            skip_list,
            wal,
            max_version: TxnTs::default(),
            buf: Vec::with_capacity(page_size()),
            memtable_size: self.memtable_size,
            read_only: self.read_only,
        };
        memtable.reload()?;
        Ok(memtable)
    }
}
impl<T> Memtable for MorsMemtable<T>
where
    T: SkipList,
{
    fn insert(&mut self, key: String, value: String) {
        todo!()
    }

    fn get(&self, key: &str) -> Option<&str> {
        todo!()
    }

    fn remove(&mut self, key: &str) -> Option<String> {
        todo!()
    }

    fn size(&self) -> usize {
        todo!()
    }
}
