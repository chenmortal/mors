use std::collections::VecDeque;
use std::fs::read_dir;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use memmap2::Advice;
use mors_common::mmap::MmapFileBuilder;
use mors_common::page_size;
use mors_traits::file_id::{FileId, MemtableId};
use mors_traits::kms::{Kms, KmsCipher};
use mors_traits::memtable::MemtableBuilder;
use mors_traits::skip_list::SkipList;
use mors_traits::ts::{KeyTsBorrow, TxnTs};
use mors_wal::error::MorsWalError;
use mors_wal::LogFile;

use crate::error::MorsMemtableError;
use crate::Result;
use crate::DEFAULT_DIR;

pub struct MorsMemtable<T: SkipList, K: Kms> {
    pub(crate) skip_list: T,
    pub(crate) wal: LogFile<MemtableId, K>,
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
}
impl<T: SkipList, K: Kms> MemtableBuilder<MorsMemtable<T, K>, K>
    for MorsMemtableBuilder<T>
where
    MorsMemtableError: From<<T as SkipList>::ErrorType>,
    MorsWalError: From<<K as Kms>::ErrorType>
        + From<<<K as Kms>::Cipher as KmsCipher>::ErrorType>,
{
    fn open(&self, kms: K, id: MemtableId) -> Result<MorsMemtable<T, K>> {
        let mut mmap_builder = MmapFileBuilder::new();
        mmap_builder
            .advice(Advice::Sequential)
            .read(true)
            .write(!self.read_only);

        let mem_path = id.join_dir(self.dir.clone());
        let skip_list = T::new(self.arena_size(), KeyTsBorrow::cmp)?;

        let wal = LogFile::open(
            id,
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

    fn open_exist(&self, kms: K) -> Result<VecDeque<Arc<MorsMemtable<T, K>>>> {
        let mut ids = read_dir(&self.dir)?
            .filter_map(std::result::Result::ok)
            .filter_map(|e| MemtableId::parse(e.path()).ok())
            .collect::<Vec<_>>();
        ids.sort();

        let mut immut_memtable = VecDeque::with_capacity(self.num_memtables);

        for id in ids.iter() {
            let memtable = self.open(kms.clone(), *id)?;
            if memtable.skip_list.is_empty() {
                continue;
            };
            immut_memtable.push_back(Arc::new(memtable));
        }
        if !ids.is_empty() {
            self.next_fid
                .store((*ids.last().unwrap()).into(), Ordering::SeqCst);
        }
        self.next_fid.fetch_add(1, Ordering::SeqCst);
        Ok(immut_memtable)
    }

    fn build(&self, kms: K) -> Result<MorsMemtable<T, K>> {
        let id: MemtableId =
            self.next_fid.fetch_add(1, Ordering::SeqCst).into();
        let path = id.join_dir(&self.dir);
        if path.exists() {
            return Err(MorsMemtableError::FileExists(path));
        }
        self.open(kms, id)
    }
    
    fn read_only(&self)->bool {
        self.read_only
    }
}
