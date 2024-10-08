use mors_traits::kms::{CipherKeyId, EncryptError, KmsError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MorsKmsError {
    #[error(transparent)]
    IOErr(#[from] std::io::Error),
    #[error(transparent)]
    SystemTimeErr(#[from] std::time::SystemTimeError),
    #[error("Poisoned RwLock: {0}")]
    RwLockPoisoned(String),
    #[error("Encryption key mismatch")]
    EncryptionKeyMismatch,
    #[error("Invalid data key id: {0}")]
    InvalidDataKeyID(CipherKeyId),
    #[error(transparent)]
    MorsEncryptError(#[from] MorsEncryptError),
}

#[derive(Error, Debug)]
pub enum MorsEncryptError {
    #[error("Encryption key's length should be either 16 or 32 bytes")]
    InvalidEncryptionKey,
    #[error("Invalid nonce: {nonce}, plaintext: {plaintext}")]
    EncryptError { nonce: String, plaintext: String },
    #[error("Invalid nonce: {nonce}, ciphertext: {ciphertext}")]
    DecryptError { nonce: String, ciphertext: String },
}

impl From<MorsEncryptError> for EncryptError {
    fn from(err: MorsEncryptError) -> EncryptError {
        EncryptError::new(err)
    }
}
impl From<MorsEncryptError> for KmsError {
    fn from(err: MorsEncryptError) -> KmsError {
        KmsError::new(err)
    }
}
impl From<MorsKmsError> for KmsError {
    fn from(val: MorsKmsError) -> Self {
        KmsError::new(val)
    }
}
