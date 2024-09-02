use std::sync::atomic::Ordering;
use std::{collections::HashSet, sync::Arc};

use mors_common::ts::TxnTs;

use mors_traits::kms::Kms;
use mors_traits::levelctl::LevelCtlTrait;
use mors_traits::memtable::MemtableTrait;
use mors_traits::skip_list::SkipListTrait;
use mors_traits::sstable::TableTrait;
use mors_traits::vlog::VlogCtlTrait;
use parking_lot::Mutex;

use super::error::TxnError;
use super::mark::WaterMark;
use super::{Result, WriteTxn};

#[derive(Clone)]
pub struct TxnManager(Arc<TxnManagerInner>);
pub(crate) struct TxnManagerInner {
    core: parking_lot::Mutex<TxnManagerCore>,
    read_mark: WaterMark,
    txn_mark: WaterMark,
    config: TxnManagerBuilder,
    send_write_req: Mutex<()>,
}
#[derive(Debug, Default)]
pub(crate) struct TxnManagerCore {
    next: TxnTs,
    discard: TxnTs,
    last_cleanup: TxnTs,
    committed: Vec<CommittedTxn>,
}
#[derive(Debug, Default, Clone)]
struct CommittedTxn {
    ts: TxnTs,
    conflict_keys: HashSet<u64>,
}
#[derive(Debug, Clone, Copy)]
pub struct TxnManagerBuilder {
    read_only: bool,
    // DetectConflicts determines whether the transactions would be checked for
    // conflicts. The transactions can be processed at a higher rate when
    // conflict detection is disabled.
    detect_conflicts: bool,
    // Transaction start and commit timestamps are managed by end-user.
    // This is only useful for databases built on top of Badger (like Dgraph).
    // Not recommended for most users.
    managed: bool,
}
impl Default for TxnManagerBuilder {
    fn default() -> Self {
        Self {
            read_only: false,
            detect_conflicts: true,
            managed: false,
        }
    }
}

impl TxnManagerBuilder {
    pub(crate) async fn build(&self, max_version: TxnTs) -> Result<TxnManager> {
        let core = TxnManagerCore {
            next: max_version + 1,
            ..Default::default()
        };
        Ok(TxnManager(Arc::new(TxnManagerInner {
            core: parking_lot::Mutex::new(core),
            read_mark: WaterMark::new(
                "TxnManager PendingRead Process",
                max_version,
            ),
            txn_mark: WaterMark::new("TxnManager TxnTs Process", max_version),
            send_write_req: Mutex::new(()),
            config: *self,
        })))
    }
}
impl TxnManager {
    pub(super) async fn generate_read_ts(&self) -> Result<TxnTs> {
        let read_ts = {
            let core_lock = self.0.core.lock();
            core_lock.next - 1
        };
        self.0.read_mark.begin(read_ts).await?;
        self.0.txn_mark.wait_for_mark(read_ts).await?;
        Ok(read_ts)
    }
    #[allow(clippy::await_holding_lock)]
    pub(super) async fn generate_commit_ts<
        M: MemtableTrait<S, K>,
        K: Kms,
        L: LevelCtlTrait<T, K>,
        T: TableTrait<K::Cipher>,
        S: SkipListTrait,
        V: VlogCtlTrait<K>,
    >(
        &self,
        txn: &WriteTxn<M, K, L, T, S, V>,
    ) -> Result<TxnTs> {
        let read_key_hash = txn.read_key_hash.lock();
        let mut core = self.0.core.lock();

        if !read_key_hash.is_empty() {
            for committed_txn in
                core.committed.iter().filter(|c| c.ts > txn.read_ts)
            {
                for hash in read_key_hash.iter() {
                    if committed_txn.conflict_keys.contains(hash) {
                        return Err(TxnError::Conflict);
                    }
                }
            }
        }
        self.0.read_mark.done(txn.read_ts).await?;
        if self.0.config.detect_conflicts {
            let max_read_tx: TxnTs =
                self.0.read_mark.done_until().load(Ordering::Acquire).into();
            assert!(max_read_tx >= core.last_cleanup);
            if max_read_tx != core.last_cleanup {
                core.last_cleanup = max_read_tx;
                core.committed = core
                    .committed
                    .iter()
                    .filter(|txn| txn.ts > max_read_tx)
                    .cloned()
                    .collect();
            }
        }

        let commit_ts = core.next;
        core.next += 1;
        self.0.txn_mark.begin(commit_ts).await?;

        debug_assert!(commit_ts >= core.last_cleanup);

        if self.0.config.detect_conflicts {
            core.committed.push(CommittedTxn {
                ts: commit_ts,
                conflict_keys: txn.conflict_keys.clone().unwrap(),
            });
        }

        Ok(commit_ts)
    }
    pub async fn done_commit(&self, txn: TxnTs) -> Result<()> {
        self.0.txn_mark.done(txn).await
    }
    pub fn detect_conflicts(&self) -> bool {
        self.0.config.detect_conflicts
    }
}
