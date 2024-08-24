use std::collections::VecDeque;
use std::fs::create_dir;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use crate::write::WriteRequest;
use crate::Result;
use log::info;
use mors_common::closer::Closer;
use mors_common::lock::DBLockGuard;
use mors_common::lock::DBLockGuardBuilder;
use mors_common::rayon::init_global_rayon_pool;
use mors_traits::default::{WithDir, WithReadOnly, DEFAULT_DIR};
use mors_traits::kms::{Kms, KmsBuilder};
use mors_traits::levelctl::{LevelCtlBuilderTrait, LevelCtlTrait};
use mors_traits::memtable::{MemtableBuilderTrait, MemtableTrait};
use mors_traits::skip_list::SkipListTrait;
use mors_traits::sstable::TableTrait;
use mors_traits::txn::TxnManagerBuilderTrait;
use mors_traits::txn::TxnManagerTrait;
use mors_traits::vlog::VlogCtlBuilderTrait;
use mors_traits::vlog::VlogCtlTrait;
use tokio::sync::mpsc::Sender;

pub struct Core<
    M: MemtableTrait<S, K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
    S: SkipListTrait,
    V: VlogCtlTrait<K>,
> {
    inner: Arc<CoreInner<M, K, L, T, S, V>>,
}
impl<
        M: MemtableTrait<S, K>,
        K: Kms,
        L: LevelCtlTrait<T, K>,
        T: TableTrait<K::Cipher>,
        S: SkipListTrait,
        V: VlogCtlTrait<K>,
    > Clone for Core<M, K, L, T, S, V>
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<
        M: MemtableTrait<S, K>,
        K: Kms,
        L: LevelCtlTrait<T, K>,
        T: TableTrait<K::Cipher>,
        S: SkipListTrait,
        V: VlogCtlTrait<K>,
    > Core<M, K, L, T, S, V>
{
    pub(crate) fn inner(&self) -> &Arc<CoreInner<M, K, L, T, S, V>> {
        &self.inner
    }
}
impl<
        M: MemtableTrait<S, K>,
        K: Kms,
        L: LevelCtlTrait<T, K>,
        T: TableTrait<K::Cipher>,
        S: SkipListTrait,
        V: VlogCtlTrait<K>,
    > CoreInner<M, K, L, T, S, V>
{
    pub(crate) fn memtable(&self) -> Option<&RwLock<Arc<M>>> {
        self.memtable.as_ref()
    }
    pub(crate) fn read_memtable(&self) -> Result<Option<Arc<M>>> {
        if let Some(mem) = self.memtable.as_ref() {
            return Ok(Some(mem.read()?.clone()));
        }
        Ok(None)
    }
    pub(crate) fn immut_memtable(&self) -> &RwLock<VecDeque<Arc<M>>> {
        &self.immut_memtable
    }
    pub(crate) fn flush_sender(&self) -> &Sender<Arc<M>> {
        &self.flush_sender
    }
    pub(crate) fn write_sender(&self) -> &Sender<WriteRequest> {
        &self.write_sender
    }
    pub(crate) fn build_memtable(&self) -> Result<M> {
        Ok(self.memtable_builder.build(self.kms.clone())?)
    }
    pub(crate) fn kms(&self) -> &K {
        &self.kms
    }
    pub(crate) fn levelctl(&self) -> &L {
        &self.levelctl
    }
    pub(crate) fn vlogctl(&self) -> &V {
        &self.vlogctl
    }
}
pub(crate) struct CoreInner<M, K, L, T, S, V>
where
    M: MemtableTrait<S, K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
    S: SkipListTrait,
    V: VlogCtlTrait<K>,
{
    lock_guard: DBLockGuard,
    kms: K,
    immut_memtable: RwLock<VecDeque<Arc<M>>>,
    memtable: Option<RwLock<Arc<M>>>,
    memtable_builder: M::MemtableBuilder,
    levelctl: L,
    vlogctl: V,
    write_sender: Sender<WriteRequest>,
    flush_sender: Sender<Arc<M>>,
    t: PhantomData<T>,
}

pub struct CoreBuilder<
    M: MemtableTrait<S, K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
    S: SkipListTrait,
    Txn: TxnManagerTrait,
    V: VlogCtlTrait<K>,
> {
    read_only: bool,
    dir: PathBuf,
    num_memtables: usize,
    kms: K::KmsBuilder,
    memtable: M::MemtableBuilder,
    levelctl: L::LevelCtlBuilder,
    vlogctl: V::VlogCtlBuilder,
    txn_manager: Txn::TxnManagerBuilder,
}
impl<
        M: MemtableTrait<S, K>,
        K: Kms,
        L: LevelCtlTrait<T, K>,
        T: TableTrait<K::Cipher>,
        S: SkipListTrait,
        Txn: TxnManagerTrait,
        V: VlogCtlTrait<K>,
    > Default for CoreBuilder<M, K, L, T, S, Txn, V>
{
    fn default() -> Self {
        Self {
            read_only: false,
            num_memtables: 5,
            dir: PathBuf::from(DEFAULT_DIR),
            kms: K::KmsBuilder::default(),
            memtable: M::MemtableBuilder::default(),
            levelctl: L::LevelCtlBuilder::default(),
            txn_manager: Txn::TxnManagerBuilder::default(),
            vlogctl: V::VlogCtlBuilder::default(),
        }
    }
}
impl<
        M: MemtableTrait<S, K>,
        K: Kms,
        L: LevelCtlTrait<T, K>,
        T: TableTrait<K::Cipher>,
        S: SkipListTrait,
        Txn: TxnManagerTrait,
        V: VlogCtlTrait<K>,
    > CoreBuilder<M, K, L, T, S, Txn, V>
{
    fn init_dir(&mut self) {
        let default_dir = PathBuf::from(DEFAULT_DIR);
        if !self.dir.exists() {
            create_dir(&self.dir).unwrap_or_else(|_| {
                panic!("Failed to create dir: {:?}", self.dir)
            });
        }
        if self.dir != default_dir {
            if self.kms.dir() == &default_dir {
                self.kms.set_dir(self.dir.clone());
            }
            if self.memtable.dir() == &default_dir {
                self.memtable.set_dir(self.dir.clone());
            }
            if self.levelctl.dir() == &default_dir {
                self.levelctl.set_dir(self.dir.clone());
            }
            if self.vlogctl.dir() == &default_dir {
                self.vlogctl.set_dir(self.dir.clone());
            }
        }
    }
    pub(crate) fn num_memtables(&self) -> usize {
        self.num_memtables
    }
    pub fn set_num_memtables(&mut self, num_memtables: usize) -> &mut Self {
        self.num_memtables = num_memtables;
        self.memtable.set_num_memtables(num_memtables);
        self
    }
    pub fn set_memtable_size(&mut self, memtable_size: usize) -> &mut Self {
        self.memtable.set_memtable_size(memtable_size);
        self
    }
}
impl<
        M: MemtableTrait<S, K>,
        K: Kms,
        L: LevelCtlTrait<T, K>,
        T: TableTrait<K::Cipher>,
        S: SkipListTrait,
        Txn: TxnManagerTrait,
        V: VlogCtlTrait<K>,
    > CoreBuilder<M, K, L, T, S, Txn, V>
{
    pub async fn build(&mut self) -> Result<Core<M, K, L, T, S, V>> {
        self.init_dir();
        init_global_rayon_pool();

        let mut guard_builder = DBLockGuardBuilder::new();

        guard_builder.add_dir(self.dir.clone());
        guard_builder.read_only(self.read_only);

        let lock_guard = guard_builder.build()?;

        let kms = self.kms.build()?;
        let immut_memtable = self.memtable.open_exist(kms.clone())?;
        info!("open {} immut_memtable", immut_memtable.len());

        let mut memtable = None;
        if !self.memtable.read_only() {
            memtable =
                RwLock::new(Arc::new(self.memtable.build(kms.clone())?)).into();
        }
        let discard = self.vlogctl.build_discard()?;
        let levelctl = self.levelctl.build(kms.clone()).await?;

        let compact_task = Closer::new("levectl compact");
        compact_task.set_joinhandle(tokio::spawn(
            levelctl.clone().spawn_compact(
                compact_task.clone(),
                kms.clone(),
                discard,
            ),
        ));

        let mut max_version = levelctl.max_version();
        immut_memtable.iter().for_each(|m| {
            max_version = max_version.max(m.max_version());
        });

        self.txn_manager.build(max_version).await?;
        let immut_memtable = RwLock::new(immut_memtable);

        let vlogctl = self.vlogctl.build(kms.clone()).await?;
        let (write_sender, receiver) = Self::init_write_channel();
        let (flush_sender, flush_receiver) =
            Self::init_flush_channel(self.num_memtables);

        let inner = Arc::new(CoreInner {
            lock_guard,
            kms,
            immut_memtable,
            memtable,
            levelctl,
            t: PhantomData,
            write_sender,
            memtable_builder: self.memtable.clone(),
            flush_sender,
            vlogctl,
        });

        let write_task = Closer::new("write request task");
        write_task.set_joinhandle(tokio::spawn(CoreInner::do_write_task(
            inner.clone(),
            receiver,
            write_task.clone(),
        )));
        let flush_task = Closer::new("flush task");
        flush_task.set_joinhandle(tokio::spawn(CoreInner::do_flush_task(
            inner.clone(),
            flush_receiver,
            flush_task.clone(),
        )));
        let core = Core { inner };
        Ok(core)
    }
    pub fn set_read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }
    pub fn set_dir(&mut self, dir: PathBuf) -> &mut Self {
        self.dir = dir;
        self
    }
}
