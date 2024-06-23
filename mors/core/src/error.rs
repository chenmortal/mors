use mors_traits::kms::{EncryptError, KmsError};
use thiserror::Error;


#[derive(Error, Debug)]
pub enum MorsError {
    #[error("IO Error: {0}")]
    IOErr(#[from] std::io::Error),
    #[error("Error in encryption: {0}")]
    EncryptErr(#[from] EncryptError),
    #[error("Error in KMS: {0}")]
    KmsError(#[from] KmsError),
}
