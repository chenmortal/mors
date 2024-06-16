use thiserror::Error;

use mors_traits::skip_list::SkipListError;
use mors_wal::error::MorsWalError;

#[derive(Error, Debug)]
pub enum MorsMemtableError {
    #[error(transparent)]
    SkipList(#[from] SkipListError),
    #[error(transparent)]
    Wal(#[from] MorsWalError),
    #[error("IO: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Key not found")]
    KeyNotFound,
    #[error("Null Pointer Error")]
    NullPointerError,
    #[error("Log truncate required to run DB. This might result in data loss ; end offset: {0} < size: {1} ")]
    TruncateNeeded(usize, usize),
}
