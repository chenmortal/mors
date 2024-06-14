use thiserror::Error;

use crate::cipher::CipherKeyId;

#[derive(Error, Debug)]
pub enum EncryptError {
    #[error("Encryption key's length should be either 16 or 32 bytes")]
    InvalidEncryptionKey,
    #[error("Invalid nonce: {nonce}, plaintext: {plaintext}")]
    EncryptError { nonce: String, plaintext: String },
    #[error("Invalid nonce: {nonce}, ciphertext: {ciphertext}")]
    DecryptError { nonce: String, ciphertext: String },
    #[error(transparent)]
    IOErr(#[from] std::io::Error),
    #[error(transparent)]
    SystemTimeErr(#[from] std::time::SystemTimeError),
    #[error("Poisoned RwLock: {0}")]
    RwLockPoisoned(String),
    #[error("Encryption key mismatch")]
    EncryptionKeyMismatch,
    #[error("Invalid data key id {0}")]
    InvalidDataKeyID(CipherKeyId),
}
