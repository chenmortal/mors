use std::sync::PoisonError;

use mors_traits::{
    kms::{EncryptError, KmsError},
    levelctl::LevelCtlError,
    memtable::MemtableError,
    sstable::SSTableError,

    vlog::VlogError,
};
use thiserror::Error;

use crate::txn::error::TxnError;

#[derive(Error, Debug)]
pub enum MorsError {
    #[error("IO Error: {0}")]
    IOErr(#[from] std::io::Error),
    #[error("Error in encryption: {0}")]
    EncryptErr(#[from] EncryptError),
    #[error("Error in KMS: {0}")]
    KmsError(#[from] KmsError),
    #[error("LevelCtl Error: {0}")]
    LevelCtlError(#[from] LevelCtlError),
    #[error("TxnManager Error: {0}")]
    TxnManagerError(#[from] TxnError),
    #[error("Memtable Error: {0}")]
    MemtableError(#[from] MemtableError),
    #[error("SSTable Error: {0}")]
    SSTableError(#[from] SSTableError),
    #[error("Vlog Error: {0}")]
    VlogError(#[from] VlogError),
    #[error("Poisoned RwLock: {0}")]
    RwLockPoisoned(String),
    #[error("Send Error: {0}")]
    SendError(String),
    #[error("Write Request too long: {0} > {1}")]
    ToLongWriteRequest(usize, usize),
    #[error("Write Request Error: {0}")]
    WriteRequestError(String),
    #[error("Poison error: {0}")]
    PoisonError(String),
}
impl<T> From<PoisonError<T>> for MorsError {
    fn from(e: PoisonError<T>) -> MorsError {
        MorsError::PoisonError(e.to_string())
    }
}
unsafe impl Send for MorsError {}
