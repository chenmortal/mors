use std::{collections::HashSet, sync::Arc};

use mors_traits::{
    ts::TxnTs,
    txn::{TxnManagerBuilderTrait, TxnManagerError, TxnManagerTrait},
};
use parking_lot::Mutex;

use crate::{error::MorsTxnError, mark::WaterMark};

pub struct TxnManager(Arc<TxnManagerInner>);
pub(crate) struct TxnManagerInner {
    core: Mutex<TxnManagerCore>,
    read_mark: WaterMark,
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
impl TxnManagerTrait for TxnManager {
    type ErrorType = MorsTxnError;
    type TxnManagerBuilder = TxnManagerBuilder;
}
impl TxnManagerBuilderTrait<TxnManager> for TxnManagerBuilder {
    async fn build(
        &self,
        max_version: TxnTs,
    ) -> std::result::Result<TxnManager, TxnManagerError> {
        let mut core = TxnManagerCore::default();

        todo!()
    }
}
