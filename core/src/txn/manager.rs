use std::{collections::HashSet, sync::Arc};

use mors_common::ts::TxnTs;

use parking_lot::Mutex;

use super::mark::WaterMark;
use super::Result;

#[derive(Clone)]
pub struct TxnManager(Arc<TxnManagerInner>);
pub(crate) struct TxnManagerInner {
    core: tokio::sync::Mutex<TxnManagerCore>,
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
#[derive(Debug, Default)]
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
            core: tokio::sync::Mutex::new(core),
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
            let core_lock = self.0.core.lock().await;
            let read_ts = core_lock.next - 1;
            self.0.read_mark.begin(read_ts).await?;
            read_ts
        };
        self.0.txn_mark.wait_for_mark(read_ts).await?;
        Ok(read_ts)
    }
    pub fn detect_conflicts(&self) -> bool {
        self.0.config.detect_conflicts
    }
}
