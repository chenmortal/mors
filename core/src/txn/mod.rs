use ahash::RandomState;
use error::TxnError;
use manager::TxnManager;
use parking_lot::Mutex;
use tokio::sync::oneshot;

pub mod error;
pub mod manager;
mod mark;
type Result<T> = std::result::Result<T, TxnError>;

use std::collections::{HashMap, HashSet};

use std::str::from_utf8;
use std::sync::atomic::AtomicI32;

use bytes::Bytes;
use mors_common::kv::{Entry, Meta};
use mors_common::ts::{KeyTs, TxnTs};
use mors_traits::kms::Kms;
use mors_traits::levelctl::LevelCtlTrait;
use mors_traits::memtable::{MemtableBuilderTrait, MemtableTrait};
use mors_traits::skip_list::SkipListTrait;
use mors_traits::sstable::TableTrait;

use crate::core::Core;
use crate::error::MorsError;
use crate::KvEntry;
use lazy_static::lazy_static;
use mors_traits::vlog::VlogCtlTrait;

use rand::{thread_rng, Rng};

/// Prefix for internal keys used by badger.
const MORS_PREFIX: &[u8] = b"!mors!";
/// For indicating end of entries in txn.
const TXN_KEY: &[u8] = b"!mors!txn";
/// For storing the banned namespaces.
const BANNED_NAMESPACES_KEY: &[u8] = b"!mors!banned";
lazy_static! {
    pub(crate) static ref HASH: RandomState =
        ahash::RandomState::with_seed(thread_rng().gen());
}
#[derive(Debug, Clone, Copy)]
pub struct TxnConfig {
    read_only: bool,
    // DetectConflicts determines whether the transactions would be checked for
    // conflicts. The transactions can be processed at a higher rate when
    // conflict detection is disabled.
    detect_conflicts: bool,
    // Transaction start and commit timestamps are managed by end-user.
    // This is only useful for databases built on top of Badger (like Dgraph).
    // Not recommended for most users.
    managed_txns: bool,
}
pub struct WriteTxn<
    M: MemtableTrait<S, K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
    S: SkipListTrait,
    V: VlogCtlTrait<K>,
> {
    pub(crate) core: Core<M, K, L, T, S, V>,
    pub(super) read_ts: TxnTs,
    commit_ts: TxnTs,
    size: usize,
    count: usize,
    txn: TxnManager,
    conflict_keys: Option<HashSet<u64>>,
    pub(super) read_key_hash: Mutex<Vec<u64>>,
    pending_writes: HashMap<Bytes, Entry>,
    duplicate_writes: Vec<Entry>,
    num_iters: AtomicI32,
    discard: bool,
}

impl<
        M: MemtableTrait<S, K>,
        K: Kms,
        L: LevelCtlTrait<T, K>,
        T: TableTrait<K::Cipher>,
        S: SkipListTrait,
        V: VlogCtlTrait<K>,
    > WriteTxn<M, K, L, T, S, V>
{
    pub(crate) async fn new(
        core: Core<M, K, L, T, S, V>,
        custom_txn: Option<TxnTs>,
    ) -> Result<Self> {
        let txn = core.inner().txn_manager().clone();
        let read_ts = match custom_txn {
            Some(txn) => txn,
            None => txn.generate_read_ts().await?,
        };

        let conflict_keys = if txn.detect_conflicts() {
            Some(HashSet::new())
        } else {
            None
        };
        let write_txn = Self {
            read_ts,
            commit_ts: TxnTs::default(),
            size: TXN_KEY.len() + 10,
            count: 1,
            txn,
            conflict_keys,
            read_key_hash: Default::default(),
            pending_writes: HashMap::new(),
            duplicate_writes: Default::default(),
            num_iters: AtomicI32::new(0),
            discard: false,
            core,
        };
        Ok(write_txn)
    }
    pub(crate) fn modify(&mut self, mut entry: Entry) -> Result<()> {
        const MAX_KEY_SIZE: usize = 65000;
        let core_inner = self.core.inner();
        let threshold = core_inner.vlogctl().value_threshold();
        let vlog_file_size = core_inner.vlogctl().vlog_file_size();
        let max_batch_count = core_inner.memtable_builder().max_batch_count();
        let max_batch_size = core_inner.memtable_builder().max_batch_size();

        if self.discard {
            return Err(TxnError::DiscardTxn);
        }
        if entry.key().is_empty() {
            return Err(TxnError::EmptyKey);
        }
        if entry.key().starts_with(MORS_PREFIX) {
            return Err(TxnError::InvalidKey(from_utf8(MORS_PREFIX).unwrap()));
        }
        if entry.key().len() > MAX_KEY_SIZE {
            return Err(TxnError::ExceedSize(
                "Key",
                entry.key().len(),
                MAX_KEY_SIZE,
            ));
        }
        if entry.value().len() > vlog_file_size {
            return Err(TxnError::ExceedSize(
                "Value",
                entry.value().len(),
                vlog_file_size,
            ));
        }

        self.count += 1;
        if entry.value_threshold() == 0 {
            entry.set_value_threshold(threshold);
        }
        self.size += entry.estimate_size(entry.value_threshold());

        if self.count >= max_batch_count || self.size >= max_batch_size {
            return Err(TxnError::TxnTooBig);
        }

        if let Some(c) = self.conflict_keys.as_mut() {
            c.insert(HASH.hash_one(entry.key()));
        }

        let new_version = entry.version();
        if let Some(old) =
            self.pending_writes.insert(entry.key().clone(), entry)
        {
            if old.version() != new_version {
                self.duplicate_writes.push(old);
            }
        };
        Ok(())
    }
    pub(crate) async fn get(
        &self,
        key: Bytes,
    ) -> std::result::Result<KvEntry, MorsError> {
        if key.is_empty() {
            return Err(TxnError::EmptyKey.into());
        }
        if self.discard {
            return Err(TxnError::DiscardTxn.into());
        }
        if let Some(entry) = self.pending_writes.get(&key) {
            if entry.key() == &key {
                if entry.is_deleted_or_expired() {
                    return Err(TxnError::KeyNotFound.into());
                }
                let mut entry_clone = entry.clone();
                entry_clone.set_version(self.read_ts);

                let mut kv_entry: KvEntry = entry_clone.into();
                kv_entry.set_status(crate::PrefetchStatus::Prefetched);
                return Ok(kv_entry);
            }
        };
        let hash = HASH.hash_one(&key);
        {
            let mut read_key_hash = self.read_key_hash.lock();
            read_key_hash.push(hash);
        }
        let key_ts = KeyTs::new(key, self.read_ts);
        match self.core.inner().get(&key_ts).await? {
            Some((txn_ts, value)) => {
                if value.is_none() {
                    return Err(TxnError::ValueNotFound.into());
                }
                let value = value.unwrap();
                if value.meta().is_empty() || value.is_deleted_or_expired() {
                    return Err(TxnError::ValueNotFound.into());
                }
                let mut entry: Entry = (key_ts, value).into();
                entry.set_version(txn_ts);
                let kv_entry: KvEntry = entry.into();
                Ok(kv_entry)
            }
            None => Err(TxnError::KeyNotFound.into()),
        }
    }
    pub(crate) async fn commit(
        &mut self,
    ) -> std::result::Result<(), MorsError> {
        if self.pending_writes.is_empty() {
            return Ok(());
        }
        if self.discard {
            return Err(TxnError::DiscardTxn.into());
        }
        let (commit_ts, recv) = self.commit_send().await?;
        let result = recv.await;
        self.core
            .inner()
            .txn_manager()
            .done_commit(commit_ts)
            .await?;
        result.map_err(|e| MorsError::RecvError(e.to_string()))??;
        Ok(())
    }
    pub(crate) async fn commit_send(
        &mut self,
    ) -> std::result::Result<
        (TxnTs, oneshot::Receiver<std::result::Result<(), MorsError>>),
        MorsError,
    > {
        let commit_ts = self
            .core
            .inner()
            .txn_manager()
            .generate_commit_ts(self)
            .await?;

        let mut keep_together = true;
        for entry in self
            .pending_writes
            .iter_mut()
            .map(|x| x.1)
            .chain(self.duplicate_writes.iter_mut())
        {
            if entry.version().is_empty() {
                entry.set_version(commit_ts);
            } else {
                keep_together = false;
            }
        }

        let mut entries = Vec::with_capacity(
            self.pending_writes.len() + self.duplicate_writes.len() + 1,
        );
        for mut entry in self
            .pending_writes
            .drain()
            .map(|x| x.1)
            .chain(self.duplicate_writes.drain(..))
        {
            if keep_together {
                entry.meta_mut().insert(Meta::TXN);
            }
            entries.push(entry);
        }

        if keep_together {
            debug_assert!(!commit_ts.is_empty());
            let mut entry = Entry::new(
                TXN_KEY.into(),
                commit_ts.to_u64().to_string().into(),
            );
            entry.set_version(commit_ts);
            entry.set_meta(Meta::FIN_TXN);
            entries.push(entry);
        }
        let r = match self.core.inner().send_to_write_channel(entries).await {
            Ok(r) => r,
            Err(e) => {
                self.core
                    .inner()
                    .txn_manager()
                    .done_commit(commit_ts)
                    .await?;
                return Err(e);
            }
        };
        Ok((commit_ts, r))
    }
}
// impl<
//         M: MemtableTrait<S, K>,
//         K: Kms,
//         L: LevelCtlTrait<T, K>,
//         T: TableTrait<K::Cipher>,
//         S: SkipListTrait,
//         V: VlogCtlTrait<K>,
//     > Core<M, K, L, T, S, V>
// {
//     pub(crate) async fn begin_write(&self) -> WriteTxn<M, K, L, T, S, V> {
//         WriteTxn::new(self.clone(), None)
//     }
// }
