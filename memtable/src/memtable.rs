use log::info;
use std::collections::VecDeque;
use std::fs::{read_dir, remove_file};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use memmap2::Advice;
use mors_common::mmap::MmapFileBuilder;
use mors_common::page_size;
use mors_traits::default::{WithDir, WithReadOnly, DEFAULT_DIR};
use mors_traits::file_id::{FileId, MemtableId};
use mors_traits::kms::Kms;
use mors_traits::memtable::MemtableBuilderTrait;
use mors_traits::skip_list::SkipListTrait;
use mors_traits::ts::{KeyTsBorrow, TxnTs};

use mors_wal::LogFile;

use crate::error::MorsMemtableError;
use crate::Result;

pub struct Memtable<T: SkipListTrait, K: Kms> {
    pub(crate) skip_list: T,
    pub(crate) wal: LogFile<MemtableId, K>,
    pub(crate) max_version: TxnTs,
    pub(crate) buf: Vec<u8>,
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
// opt.maxBatchSize = (15 * opt.MemTableSize) / 100
// opt.maxBatchCount = opt.maxBatchSize / int64(skl.MaxNodeSize)
impl<T: SkipListTrait> MemtableBuilder<T>
// where
//     MorsMemtableError: From<<T as SkipListTrait>::ErrorType>,
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
impl<T: SkipListTrait> MemtableBuilder<T> {
    pub(crate) fn open_impl<K: Kms>(
        &self,
        kms: K,
        id: MemtableId,
    ) -> Result<Memtable<T, K>> {
        let mut mmap_builder = MmapFileBuilder::new();
        mmap_builder
            .advice(Advice::Sequential)
            .read(true)
            .create(!self.read_only)
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
        let memtable = Memtable {
            skip_list,
            wal,
            max_version: TxnTs::default(),
            buf: Vec::with_capacity(page_size()),
            memtable_size: self.memtable_size,
            read_only: self.read_only,
        };
        Ok(memtable)
    }

    pub fn open_exist_impl<K: Kms>(
        &self,
        kms: K,
    ) -> Result<VecDeque<Arc<Memtable<T, K>>>> {
        let mut ids = read_dir(&self.dir)?
            .filter_map(std::result::Result::ok)
            .filter_map(|e| MemtableId::parse(e.path()).ok())
            .collect::<Vec<_>>();
        ids.sort();

        let mut immut_memtable = VecDeque::with_capacity(self.num_memtables);

        let mut valid_ids = Vec::with_capacity(ids.len());
        for id in ids {
            let memtable = self.open(kms.clone(), id)?;
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

    pub fn build_impl<K: Kms>(&self, kms: K) -> Result<Memtable<T, K>> {
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
