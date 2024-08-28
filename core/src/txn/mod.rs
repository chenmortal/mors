use error::TxnManageError;
use manager::TxnManager;

pub mod error;
pub mod manager;
mod mark;
type Result<T> = std::result::Result<T, TxnManageError>;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI32};

use bytes::Bytes;
use mors_common::kv::Entry;
use mors_common::ts::TxnTs;
use mors_traits::kms::Kms;
use mors_traits::levelctl::LevelCtlTrait;
use mors_traits::memtable::MemtableTrait;
use mors_traits::skip_list::SkipListTrait;
use mors_traits::sstable::TableTrait;

use mors_traits::vlog::VlogCtlTrait;

use parking_lot::Mutex;

use crate::core::Core;

/// Prefix for internal keys used by badger.
const MORS_PREFIX: &[u8] = b"!mors!";
/// For indicating end of entries in txn.
const TXN_KEY: &[u8] = b"!mors!txn";
/// For storing the banned namespaces.
const BANNED_NAMESPACES_KEY: &[u8] = b"!mors!banned";
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
    core: Core<M, K, L, T, S, V>,
    read_ts: TxnTs,
    commit_ts: TxnTs,
    size: usize,
    count: usize,
    txn: TxnManager,
    conflict_keys: Option<HashSet<u64>>,
    read_key_hash: Mutex<Vec<u64>>,
    pending_writes: HashMap<Bytes, Entry>,
    duplicate_writes: Vec<Entry>,
    num_iters: AtomicI32,
    discard: bool,
    done_read: AtomicBool,
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
    pub async fn new(
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
            read_key_hash: Mutex::new(Vec::new()),
            pending_writes: HashMap::new(),
            duplicate_writes: Default::default(),
            num_iters: AtomicI32::new(0),
            discard: false,
            done_read: AtomicBool::new(false),
            core,
        };
        Ok(write_txn)
    }
    pub async fn modify() {}
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
    pub(crate) async fn begin_write(&self) {
        // WriteTxn::new(custom_txn);
    }
}
