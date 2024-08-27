use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI32};

use bytes::Bytes;
use mors_common::kv::Entry;
use mors_common::ts::TxnTs;
use mors_traits::core::CoreTrait;
use parking_lot::Mutex;

use crate::manager::TxnManager;
use crate::Result;
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
pub struct WriteTxn<C: CoreTrait> {
    read_ts: TxnTs,
    commit_ts: TxnTs,
    size: usize,
    count: usize,
    core: C,
    txn: TxnManager,
    conflict_keys: Option<HashSet<u64>>,
    read_key_hash: Mutex<Vec<u64>>,
    pending_writes: HashMap<Bytes, Entry>,
    duplicate_writes: Vec<Entry>,
    num_iters: AtomicI32,
    discard: bool,
    done_read: AtomicBool,
}
impl<C: CoreTrait> WriteTxn<C> {
    pub async fn new(
        core: C,
        txn: TxnManager,
        custom_txn: Option<TxnTs>,
    ) -> Result<WriteTxn<C>> {
        let read_ts = match custom_txn {
            Some(txn) => txn,
            None => txn.generate_read_ts().await?,
        };

        let conflict_keys = if txn.detect_conflicts() {
            Some(HashSet::new())
        } else {
            None
        };
        let write_txn = WriteTxn {
            read_ts,
            commit_ts: TxnTs::default(),
            size: TXN_KEY.len() + 10,
            count: 1,
            core,
            txn,
            conflict_keys,
            read_key_hash: Mutex::new(Vec::new()),
            pending_writes: HashMap::new(),
            duplicate_writes: Default::default(),
            num_iters: AtomicI32::new(0),
            discard: false,
            done_read: AtomicBool::new(false),
        };
        Ok(write_txn)
    }
}
