use thiserror::Error;

use mors_encrypt::error::EncryptError;

#[derive(Error, Debug)]
pub enum MorsWalError {
    #[error("Encryption: {0}")]
    EncryptError(#[from] EncryptError),
    #[error("IO: {0}")]
    IoError(#[from] std::io::Error),
}
