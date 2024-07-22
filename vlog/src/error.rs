use std::sync::PoisonError;

use mors_common::file_id::VlogId;
use mors_traits::vlog::VlogError;
use mors_wal::error::MorsWalError;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
#[derive(Error, Debug)]
pub enum MorsVlogError {
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("LogFile error: {0}")]
    LogFileError(#[from] MorsWalError),
    #[error("Poison error: {0}")]
    PoisonError(String),
    #[error("Log not found: {0}")]
    LogNotFound(VlogId),
    #[error("Threshold error: {0}")]
    ThresholdError(String),
    #[error("Send error: {0}")]
    SendError(String),
}
impl From<MorsVlogError> for VlogError {
    fn from(e: MorsVlogError) -> VlogError {
        VlogError::new(e)
    }
}
impl<T> From<PoisonError<T>> for MorsVlogError {
    fn from(e: PoisonError<T>) -> MorsVlogError {
        MorsVlogError::PoisonError(e.to_string())
    }
}
impl<T> From<SendError<T>> for MorsVlogError {
    fn from(value: SendError<T>) -> Self {
        MorsVlogError::SendError(value.to_string())
    }
}
