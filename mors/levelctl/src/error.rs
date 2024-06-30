use mors_traits::{kms::KmsError, sstable::SSTableError};
use thiserror::Error;

use crate::manifest::error::ManifestError;


#[derive(Error, Debug)]
pub enum MorsLevelCtlError {
    #[error("IO Error: {0}")]
    IOErr(#[from] std::io::Error),
    #[error("Manifest Error: {0}")]
    ManifestErr(#[from] ManifestError),
    #[error("Acquire Error: {0}")]
    AcquireError(#[from] tokio::sync::AcquireError),
    #[error("Kms Error: {0}")]
    KmsError(#[from] KmsError),
    #[error("SSTable Error: {0}")]
    SSTableError(#[from] SSTableError),
    #[error("Join Error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}
unsafe impl Send for MorsLevelCtlError {
    
}
