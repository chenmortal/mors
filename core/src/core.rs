use std::collections::VecDeque;
use std::fs::create_dir;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use crate::write::WriteRequest;
use crate::Result;
use mors_common::closer::Closer;
use mors_common::lock::DBLockGuard;
use mors_common::lock::DBLockGuardBuilder;
use mors_traits::default::{WithDir, WithReadOnly, DEFAULT_DIR};
use mors_traits::kms::{Kms, KmsBuilder};
use mors_traits::levelctl::{LevelCtlBuilderTrait, LevelCtlTrait};
use mors_traits::memtable::{MemtableBuilderTrait, MemtableTrait};
use mors_traits::sstable::TableTrait;
use mors_traits::txn::TxnManagerBuilderTrait;
use mors_traits::txn::TxnManagerTrait;
use tokio::sync::mpsc::Sender;

pub struct Core<
    M: MemtableTrait<K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
> {
    inner: Arc<CoreInner<M, K, L, T>>,
}
impl<
        M: MemtableTrait<K>,
        K: Kms,
        L: LevelCtlTrait<T, K>,
        T: TableTrait<K::Cipher>,
    > Clone for Core<M, K, L, T>
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<
        M: MemtableTrait<K>,
        K: Kms,
        L: LevelCtlTrait<T, K>,
        T: TableTrait<K::Cipher>,
    > CoreInner<M, K, L, T>
{
    pub(crate) fn memtable(&self) -> Option<&Arc<RwLock<M>>> {
        self.memtable.as_ref()
    }
    pub(crate) fn build_memtable(&self) -> Result<M> {
        Ok(self.memtable_builder.build(self.kms.clone())?)
    }
}
pub(crate) struct CoreInner<M, K, L, T>
where
    M: MemtableTrait<K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
{
    lock_guard: DBLockGuard,
    kms: K,
    immut_memtable: RwLock<VecDeque<Arc<M>>>,
    memtable: Option<Arc<RwLock<M>>>,
    memtable_builder: M::MemtableBuilder,
    levelctl: L,
    write_sender: Sender<WriteRequest>,
    flush_sender: Sender<Arc<M>>,
    t: PhantomData<T>,
}

pub struct CoreBuilder<
    M: MemtableTrait<K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
    Txn: TxnManagerTrait,
> {
    read_only: bool,
    dir: PathBuf,
    num_memtables: usize,
    kms: K::KmsBuilder,
    memtable: M::MemtableBuilder,
    levelctl: L::LevelCtlBuilder,
    txn_manager: Txn::TxnManagerBuilder,
}
impl<
        M: MemtableTrait<K>,
        K: Kms,
        L: LevelCtlTrait<T, K>,
        T: TableTrait<K::Cipher>,
        Txn: TxnManagerTrait,
    > Default for CoreBuilder<M, K, L, T, Txn>
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
        }
    }
}
impl<
        M: MemtableTrait<K>,
        K: Kms,
        L: LevelCtlTrait<T, K>,
        T: TableTrait<K::Cipher>,
        Txn: TxnManagerTrait,
    > CoreBuilder<M, K, L, T, Txn>
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
        }
    }
    pub(crate) fn num_memtables(&self) -> usize {
        self.num_memtables
    }
}
impl<
        M: MemtableTrait<K>,
        K: Kms,
        L: LevelCtlTrait<T, K>,
        T: TableTrait<K::Cipher>,
        Txn: TxnManagerTrait,
    > CoreBuilder<M, K, L, T, Txn>
{
    pub async fn build(&mut self) -> Result<Core<M, K, L, T>> {
        self.init_dir();
        let mut guard_builder = DBLockGuardBuilder::new();

        guard_builder.add_dir(self.dir.clone());
        guard_builder.read_only(self.read_only);

        let lock_guard = guard_builder.build()?;

        let kms = self.kms.build()?;
        let immut_memtable = self.memtable.open_exist(kms.clone())?;

        let mut memtable = None;
        if !self.memtable.read_only() {
            memtable =
                Arc::new(RwLock::new(self.memtable.build(kms.clone())?)).into();
        }
        let levelctl = self.levelctl.build(kms.clone()).await?;

        let mut max_version = levelctl.max_version();
        immut_memtable.iter().for_each(|m| {
            max_version = max_version.max(m.max_version());
        });

        self.txn_manager.build(max_version).await?;
        let immut_memtable = RwLock::new(immut_memtable);
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
        });
        let write_task = Closer::new("write request task".to_owned());
        write_task.set_joinhandle(tokio::spawn(CoreInner::do_write_task(
            inner.clone(),
            receiver,
            write_task.clone(),
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
