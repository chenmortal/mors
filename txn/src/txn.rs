use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI32};

use mors_common::kv::Entry;
use mors_common::ts::TxnTs;
use mors_traits::core::CoreTrait;
use parking_lot::Mutex;

use crate::manager::TxnManager;

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
    config: TxnConfig,
    core: C,
    txn: TxnManager,
    conflict_keys: Option<HashSet<u64>>,
    read_key_hash: Mutex<Vec<u64>>,
    duplicate_writes: Vec<Entry>,
    num_iters: AtomicI32,
    discard: bool,
    done_read: AtomicBool,
    update: bool,
}
impl<C: CoreTrait> WriteTxn<C> {
    pub async fn new(txn: TxnManager, custom_txn: Option<TxnTs>) {
        let read_ts = match custom_txn {
            Some(txn) => txn,
            None => txn.generate_read_ts().await,
        };
    }
}
