use std::error::Error;
use std::{
    fmt::Display,
    ops::{Add, AddAssign},
};
use thiserror::Error;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CipherKeyId(u64);
impl Display for CipherKeyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CipherKeyId{:x}", self.0)
    }
}
impl From<u64> for CipherKeyId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}
impl From<CipherKeyId> for u64 {
    fn from(value: CipherKeyId) -> Self {
        value.0
    }
}

impl Add<u64> for CipherKeyId {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        (self.0 + rhs).into()
    }
}
impl AddAssign<u64> for CipherKeyId {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs
    }
}
pub trait Kms: Clone + Send + Sync + 'static {
    type ErrorType: Into<KmsError>;
    type Cipher: KmsCipher;
    type KmsBuilder: KmsBuilder<Self>;
    fn get_cipher(
        &self,
        key_id: CipherKeyId,
    ) -> Result<Option<Self::Cipher>, KmsError>;
    fn latest_cipher(&self) -> Result<Option<Self::Cipher>, KmsError>;
    const NONCE_SIZE: usize;
}
pub trait KmsCipher: Send + Sync+'static {
    type ErrorType: Into<EncryptError>;

    fn cipher_key_id(&self) -> CipherKeyId;

    fn generate_nonce() -> Vec<u8>;

    fn decrypt_with_slice(
        &self,
        nonce: &[u8],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, EncryptError>;

    fn encrypt_with_slice(
        &self,
        nonce: &[u8],
        plaintext: &[u8],
    ) -> Result<Vec<u8>, EncryptError>;

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, EncryptError>;
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, EncryptError>;
}
pub trait KmsBuilder<K: Kms>: Default {
    fn build(&self) -> Result<K, KmsError>;
}
#[derive(Error, Debug)]
pub struct KmsError(Box<dyn Error>);
impl KmsError {
    pub fn new<E: Error + 'static>(error: E) -> Self {
        KmsError(Box::new(error))
    }
}
impl Display for KmsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Kms Error: {}", self.0)
    }
}
#[derive(Error, Debug)]
pub struct EncryptError(Box<dyn Error>);
impl EncryptError {
    pub fn new<E: Error + 'static>(error: E) -> Self {
        EncryptError(Box::new(error))
    }
}
impl Display for EncryptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EncryptError Error: {}", self.0)
    }
}
