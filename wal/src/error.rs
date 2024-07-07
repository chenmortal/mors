use std::path::PathBuf;
use mors_traits::kms::{EncryptError, KmsError};
use thiserror::Error;
use mors_traits::file_id::FileId;


#[derive(Error, Debug)]
pub enum MorsWalError {
    #[error(transparent)]
    KmsError(#[from] KmsError),
    #[error(transparent)]
    EncryptError(#[from] EncryptError),
    #[error("IO: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Invalid log header {0:?}, you may need to delete the file and try again.")]
    InvalidLogHeader(PathBuf),
}
