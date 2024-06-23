use mors_traits::kms::{EncryptError, KmsError};
use thiserror::Error;


#[derive(Error, Debug)]
pub enum MorsWalError {
    #[error(transparent)]
    KmsError(#[from] KmsError),
    #[error(transparent)]
    EncryptError(#[from] EncryptError),
    #[error("IO: {0}")]
    IoError(#[from] std::io::Error),
}
