use log::info;
use mors_traits::file::{StorageBuilderTrait, StorageTrait};
use std::collections::VecDeque;
use std::fs::{read_dir, remove_file};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

// use memmap2::Advice;
use mors_common::file_id::{FileId, MemtableId};
use mors_traits::memtable::MemtableBuilderTrait;
// use mors_common::page_size;
use mors_common::ts::KeyTsBorrow;
use mors_traits::{
    default::{WithDir, WithReadOnly, DEFAULT_DIR},
    kms::Kms,
    skip_list::SkipListTrait,
};

use mors_wal::LogFile;

use crate::error::MorsMemtableError;
use crate::Result;

pub struct Memtable<T: SkipListTrait, K: Kms, S: StorageTrait> {
    pub(crate) skip_list: T,
    pub(crate) wal: LogFile<MemtableId, K, S>,
    // pub(crate) max_version: TxnTs,
    pub(crate) max_txn_ts: AtomicU64,
    // pub(crate) buf: Vec<u8>,
    pub(crate) memtable_size: usize,
    pub(crate) read_only: bool,
}
pub struct MemtableBuilder<T: SkipListTrait> {
    dir: PathBuf,
    read_only: bool,
    memtable_size: usize,
    num_memtables: usize,
    next_fid: Arc<AtomicU32>,
    t: PhantomData<T>,
}
impl<T: SkipListTrait> Default for MemtableBuilder<T> {
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
impl<T: SkipListTrait> Clone for MemtableBuilder<T> {
    fn clone(&self) -> Self {
        Self {
            dir: self.dir.clone(),
            read_only: self.read_only,
            memtable_size: self.memtable_size,
            num_memtables: self.num_memtables,
            next_fid: self.next_fid.clone(),
            t: self.t,
        }
    }
}
// opt.maxBatchSize = (15 * opt.MemTableSize) / 100
// opt.maxBatchCount = opt.maxBatchSize / int64(skl.MaxNodeSize)
impl<T: SkipListTrait> MemtableBuilder<T> {
    fn arena_size(&self) -> usize {
        self.memtable_size + 2 * self.max_batch_size_impl()
    }
    pub(crate) fn max_batch_size_impl(&self) -> usize {
        (15 * self.memtable_size) / 100
    }
    #[allow(dead_code)]
    pub(crate) fn max_batch_count_impl(&self) -> usize {
        self.max_batch_size_impl() / T::MAX_NODE_SIZE
    }
    #[inline]
    pub(crate) fn set_num_memtables_impl(&mut self, num_memtables: usize) {
        self.num_memtables = num_memtables;
    }
    #[inline]
    pub(crate) fn set_memtable_size_impl(&mut self, memtable_size: usize) {
        self.memtable_size = memtable_size;
    }
}
impl<T: SkipListTrait> MemtableBuilder<T> {
    pub(crate) fn open_impl<K: Kms, S: StorageTrait>(
        &self,
        kms: K,
        id: MemtableId,
    ) -> Result<Memtable<T, K, S>> {
        let mut builder = S::StorageBuilder::default();
        // let mut mmap_builder = MmapFileBuilder::new();
        builder
            .read(true)
            .create(!self.read_only)
            .write(!self.read_only);
        // .advice(Advice::Sequential)

        let mem_path = id.join_dir(self.dir.clone());
        let skip_list = T::new(self.arena_size(), KeyTsBorrow::cmp)?;

        let wal = LogFile::open(
            id,
            mem_path,
            2 * self.memtable_size as u64,
            builder,
            kms,
        )?;
        let memtable = Memtable {
            skip_list,
            wal,
            // max_version: TxnTs::default(),
            // buf: Vec::with_capacity(page_size()),
            memtable_size: self.memtable_size,
            read_only: self.read_only,
            max_txn_ts: AtomicU64::new(0),
        };
        Ok(memtable)
    }

    pub fn open_exist_impl<K: Kms, S: StorageTrait>(
        &self,
        kms: K,
    ) -> Result<VecDeque<Arc<Memtable<T, K, S>>>> {
        let mut ids = read_dir(&self.dir)?
            .filter_map(std::result::Result::ok)
            .filter_map(|e| MemtableId::parse(e.path()).ok())
            .collect::<Vec<_>>();
        ids.sort();

        let mut immut_memtable = VecDeque::with_capacity(self.num_memtables);

        let mut valid_ids = Vec::with_capacity(ids.len());
        for id in ids {
            let mut memtable = self.open(kms.clone(), id)?;
            memtable.reload()?;
            if memtable.skip_list.is_empty() {
                let path = id.join_dir(&self.dir);
                info!("Empty memtable wal: {:?}, now delete it", path);
                remove_file(&path)?;
                info!("Deleted empty memtable wal: {:?}", path);
                continue;
            };
            immut_memtable.push_back(Arc::new(memtable));
            valid_ids.push(id);
        }
        valid_ids.sort();
        if !valid_ids.is_empty() {
            self.next_fid
                .store((*valid_ids.last().unwrap()).into(), Ordering::SeqCst);
        }
        self.next_fid.fetch_add(1, Ordering::SeqCst);
        Ok(immut_memtable)
    }

    pub fn build_impl<K: Kms, S: StorageTrait>(
        &self,
        kms: K,
    ) -> Result<Memtable<T, K, S>> {
        let id: MemtableId =
            self.next_fid.fetch_add(1, Ordering::SeqCst).into();
        let path = id.join_dir(&self.dir);
        if path.exists() {
            return Err(MorsMemtableError::FileExists(path));
        }
        Ok(self.open(kms, id)?)
    }
}
impl<T: SkipListTrait> WithDir for MemtableBuilder<T> {
    fn set_dir(&mut self, dir: PathBuf) -> &mut Self {
        self.dir = dir;
        self
    }

    fn dir(&self) -> &PathBuf {
        &self.dir
    }
}
impl<T: SkipListTrait> WithReadOnly for MemtableBuilder<T> {
    fn set_read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }

    fn read_only(&self) -> bool {
        self.read_only
    }
}
