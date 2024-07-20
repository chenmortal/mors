use mors_common::file_id::VlogId;
use mors_traits::vlog::VlogError;
use mors_wal::error::MorsWalError;
use thiserror::Error;
#[derive(Error, Debug)]
pub enum MorsVlogError {
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("LogFile error: {0}")]
    LogFileError(#[from] MorsWalError),
    #[error("Poison error: {0}")]
    PosionError(String),
    #[error("Log not found: {0}")]
    LogNotFound(VlogId),
}
impl From<MorsVlogError> for VlogError {
    fn from(e: MorsVlogError) -> VlogError {
        VlogError::new(e)
    }
}
