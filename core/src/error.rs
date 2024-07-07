use mors_traits::{
    kms::{EncryptError, KmsError},
    levelctl::LevelCtlError,
    memtable::MemtableError,
    txn::TxnManagerError,
};
use thiserror::Error;

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
    TxnManagerError(#[from] TxnManagerError),
    #[error("Memtable Error: {0}")]
    MemtableError(#[from] MemtableError),
}
unsafe impl Send for MorsError {}