use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use crate::error::MorsError;
use crate::Result;
use mors_common::lock::DBLockGuard;
use mors_common::lock::DBLockGuardBuilder;
use mors_traits::kms::{Kms, KmsBuilder};
use mors_traits::memtable::{Memtable, MemtableBuilder};
pub struct Mors<M: Memtable<K>, K: Kms> {
    pub(crate) core: Core<M, K>,
}

pub struct Core<M: Memtable<K>, K: Kms> {
    lock_guard: DBLockGuard,
    kms: K,
    immut_memtable: VecDeque<Arc<M>>,
    memtable: Option<Arc<RwLock<M>>>,
}

pub struct DBCoreBuilder {}
pub struct MorsBuilder<M: Memtable<K>, K: Kms> {
    read_only: bool,
    dir: PathBuf,
    kms: K::KmsBuilder,
    memtable: M::MemtableBuilder,
}
impl<M: Memtable<K>, K: Kms> Default for MorsBuilder<M, K> {
    fn default() -> Self {
        Self {
            read_only: false,
            dir: PathBuf::new(),
            kms: K::KmsBuilder::default(),
            memtable: M::MemtableBuilder::default(),
        }
    }
}
impl<M: Memtable<K>, K: Kms> MorsBuilder<M, K>
where
    MorsError:
        From<<K as Kms>::ErrorType> + From<<M as Memtable<K>>::ErrorType>,
{
    pub fn build(&self) -> Result<Mors<M, K>> {
        let mut guard_builder = DBLockGuardBuilder::new();

        guard_builder.add_dir(self.dir.clone());
        guard_builder.read_only(self.read_only);

        let lock_guard = guard_builder.build()?;

        let kms = self.kms.build()?;
        let immut_memtable = self.memtable.open_exist(kms.clone())?;

        let mut memtable = None;
        if !self.memtable.read_only() {
            memtable =
                Arc::new(RwLock::new(self.memtable.new(kms.clone())?)).into();
        }
        Ok(Mors {
            core: Core {
                lock_guard,
                kms,
                immut_memtable,
                memtable,
            },
        })
    }
}
