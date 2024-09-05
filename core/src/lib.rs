use core::{Core, CoreBuilder};
use std::ops::{Deref, DerefMut};

use std::time::{Duration, SystemTime};

use bytes::Bytes;
use error::MorsError;

use mors_common::ts::PhyTs;
use mors_encrypt::{cipher::AesCipher, registry::MorsKms};
use mors_levelctl::ctl::LevelCtl;
use mors_memtable::memtable::Memtable;

use mors_skip_list::skip_list::SkipList;
use mors_sstable::cache::MorsCacheBuilder;
use mors_sstable::table::Table;
use mors_traits::cache::CacheBuilder;
use mors_traits::levelctl::LevelCtlBuilderTrait;
use mors_vlog::vlogctl::VlogCtl;
use mors_wal::storage::mmap::MmapFile;
use tokio::runtime::Builder;
#[cfg(feature = "sync")]
use {std::sync::Arc, tokio::runtime::Handle};

use txn::WriteTxn;
pub mod core;
mod error;
mod flush;
mod read;
mod test;
mod txn;
mod write;
use mors_common::kv::{Entry, Meta};
pub type Result<T> = std::result::Result<T, MorsError>;

type MorsMemtable = Memtable<SkipList, MorsKms, MmapFile>;
type MorsLevelCtl = LevelCtl<Table<AesCipher>, MorsKms>;
type MorsTable = Table<AesCipher>;
type MorsLevelCtlType = LevelCtl<MorsTable, MorsKms>;
type MorsVlog = VlogCtl<MorsKms, MmapFile>;
type WriteTxnType = WriteTxn<
    MorsMemtable,
    MorsKms,
    MorsLevelCtlType,
    MorsTable,
    SkipList,
    MorsVlog,
>;
pub struct WriteTransaction {
    txn: WriteTxnType,
    #[cfg(feature = "sync")]
    handler: Handle,
}
impl Deref for WriteTransaction {
    type Target = WriteTxnType;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}
#[derive(Clone)]
pub struct Mors {
    #[cfg(feature = "sync")]
    inner: Arc<MorsInner>,
    #[cfg(not(feature = "sync"))]
    inner: MorsInner,
}

struct MorsInner {
    core: Core<
        MorsMemtable,
        MorsKms,
        MorsLevelCtl,
        Table<AesCipher>,
        SkipList,
        MorsVlog,
    >,
    #[cfg(feature = "sync")]
    runtime: tokio::runtime::Runtime,
}
#[cfg(not(feature = "sync"))]
impl Clone for MorsInner {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
        }
    }
}

pub struct MorsBuilder {
    builder: CoreBuilder<
        MorsMemtable,
        MorsKms,
        MorsLevelCtlType,
        MorsTable,
        SkipList,
        MorsVlog,
    >,
    #[cfg(feature = "sync")]
    tokio_builder: tokio::runtime::Builder,
}

#[allow(clippy::derivable_impls)]
impl Default for MorsBuilder {
    fn default() -> Self {
        let mut tokio_builder = Builder::new_multi_thread();
        tokio_builder.enable_all();
        Self {
            builder: Default::default(),
            #[cfg(feature = "sync")]
            tokio_builder,
        }
    }
}
impl Deref for Mors {
    type Target = Core<
        MorsMemtable,
        MorsKms,
        LevelCtl<Table<AesCipher>, MorsKms>,
        Table<AesCipher>,
        SkipList,
        MorsVlog,
    >;

    fn deref(&self) -> &Self::Target {
        &self.inner.core
    }
}
impl Deref for MorsBuilder {
    type Target = CoreBuilder<
        MorsMemtable,
        MorsKms,
        LevelCtl<Table<AesCipher>, MorsKms>,
        Table<AesCipher>,
        SkipList,
        MorsVlog,
    >;

    fn deref(&self) -> &Self::Target {
        &self.builder
    }
}
impl DerefMut for MorsBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.builder
    }
}
impl MorsBuilder {
    fn init(&mut self) {
        let cache_builder = MorsCacheBuilder::default();
        let cache = cache_builder.build().unwrap();
        self.levelctl.set_cache(cache);
    }
    #[cfg(feature = "sync")]
    pub fn tokio_builder(&mut self) -> &mut Builder {
        &mut self.tokio_builder
    }

    #[cfg(feature = "sync")]
    pub fn build(&mut self) -> Result<Mors> {
        use std::sync::Arc;
        self.init();
        let runtime = self.tokio_builder.build()?;
        let k = runtime.block_on(self.builder.build())?;
        let inner = MorsInner { core: k, runtime };
        Ok(Mors {
            inner: Arc::new(inner),
        })
    }
    #[cfg(not(feature = "sync"))]
    pub async fn build(&mut self) -> Result<Mors> {
        self.init();
        let core = self.builder.build().await?;
        let inner = MorsInner { core };
        Ok(Mors { inner })
    }
}
impl Mors {
    #[cfg(not(feature = "sync"))]
    pub async fn begin_write(&self) -> Result<WriteTransaction> {
        let txn = WriteTxnType::new(self.inner.core.clone(), None).await?;
        Ok(WriteTransaction { txn })
    }
    #[cfg(feature = "sync")]
    pub fn begin_write(&self) -> Result<WriteTransaction> {
        let txn = self
            .inner
            .runtime
            .block_on(WriteTxnType::new(self.inner.core.clone(), None))?;
        Ok(WriteTransaction {
            txn,
            handler: self.inner.runtime.handle().clone(),
        })
    }
}
#[derive(Debug)]
pub(crate) enum PrefetchStatus {
    Prefetched,
    NoPrefetched,
}
impl Default for PrefetchStatus {
    fn default() -> Self {
        Self::NoPrefetched
    }
}
pub struct KvEntry {
    entry: Entry,
    status: PrefetchStatus,
}
impl From<Entry> for KvEntry {
    fn from(value: Entry) -> Self {
        KvEntry {
            entry: value,
            status: PrefetchStatus::Prefetched,
        }
    }
}
impl KvEntry {
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self {
            entry: Entry::new(key, value),
            status: PrefetchStatus::default(),
        }
    }
    pub fn key(&self) -> &Bytes {
        self.entry.key()
    }
    pub fn value(&self) -> &Bytes {
        self.entry.value()
    }
    pub fn set_meta(&mut self, meta: u8) {
        self.entry.set_user_meta(meta);
    }
    pub fn meta(&self) -> u8 {
        self.entry.user_meta()
    }

    pub fn set_ttl(&mut self, ttl: Duration) {
        let expires: PhyTs = SystemTime::now()
            .checked_add(ttl)
            .unwrap()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .into();
        self.entry.set_expires_at(expires);
    }

    // WithDiscard adds a marker to Entry e. This means all the previous versions of the key (of the
    // Entry) will be eligible for garbage collection.
    // This method is only useful if you have set a higher limit for options.NumVersionsToKeep. The
    // default setting is 1, in which case, this function doesn't add any more benefit. If however, you
    // have a higher setting for NumVersionsToKeep (in Dgraph, we set it to infinity), you can use this
    // method to indicate that all the older versions can be discarded and removed during compactions.
    pub fn set_discard(&mut self) {
        self.entry.meta_mut().insert(Meta::DISCARD_EARLIER_VERSIONS);
    }

    pub fn set_merge(&mut self) {
        self.entry.meta_mut().insert(Meta::MERGE_ENTRY);
    }
    fn set_delete(&mut self) {
        self.entry.meta_mut().insert(Meta::DELETE);
    }
    pub(crate) fn set_status(&mut self, status: PrefetchStatus) {
        self.status = status;
    }
}
impl WriteTransaction {
    pub fn set(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        self.set_entry(KvEntry::new(key, value))
    }
    pub fn set_entry(&mut self, entry: KvEntry) -> Result<()> {
        Ok(self.txn.modify(entry.entry)?)
    }
    #[cfg(feature = "sync")]
    pub fn get(&self, key: Bytes) -> Result<KvEntry> {
        self.handler.block_on(self.txn.get(key))
    }
    pub fn delete(&mut self, key: Bytes) -> Result<()> {
        let mut entry = KvEntry::new(key, Bytes::new());
        entry.set_delete();
        self.set_entry(entry)
    }
    #[cfg(not(feature = "sync"))]
    pub async fn commit(&mut self) -> Result<()> {
        self.txn.commit().await
    }
    #[cfg(feature = "sync")]
    pub fn commit(&mut self) -> Result<()> {
        self.handler.block_on(self.txn.commit())
    }
}
