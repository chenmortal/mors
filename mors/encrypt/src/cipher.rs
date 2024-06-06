use crate::error::MorsEncryptError;
use aead::consts::U12;
use aead::generic_array::GenericArray;
use aead::{Aead, AeadCore, KeyInit, OsRng};
#[cfg(feature = "aes-gcm")]
use aes_gcm::{Aes128Gcm, Aes256Gcm};
#[cfg(feature = "aes-gcm-siv")]
use aes_gcm_siv::Aes128GcmSiv as Aes128Gcm;
#[cfg(feature = "aes-gcm-siv")]
use aes_gcm_siv::Aes256GcmSiv as Aes256Gcm;
use std::fmt::{Debug, Display};
use std::ops::{Add, AddAssign};

pub type Nonce = GenericArray<u8, U12>;
type Result<T> = std::result::Result<T, MorsEncryptError>;
#[derive(Debug,Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

pub(crate) enum AesCipher {
    Aes128(Aes128Gcm, CipherKeyId),
    Aes256(Aes256Gcm, CipherKeyId),
}

impl Debug for AesCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(feature = "aes-gcm")]
        match self {
            Self::Aes128(_, id) => f.debug_tuple("Aes128:").field(id).finish(),
            Self::Aes256(_, id) => f.debug_tuple("Aes256:").field(id).finish(),
        }
        #[cfg(feature = "aes-gcm-siv")]
        match self {
            Self::Aes128(_, id) => f.debug_tuple("Aes128Siv:").field(id).finish(),
            Self::Aes256(_, id) => f.debug_tuple("Aes256Siv:").field(id).finish(),
        }
    }
}

impl AesCipher {
    #[inline]
    pub(crate) fn new(key: &[u8], id: CipherKeyId) -> Result<Self> {
        let cipher = match key.len() {
            16 => Self::Aes128(Aes128Gcm::new_from_slice(key).unwrap(), id),
            32 => Self::Aes256(Aes256Gcm::new_from_slice(key).unwrap(), id),
            _ => return Err(MorsEncryptError::InvalidEncryptionKey),
        };
        Ok(cipher)
    }
    pub(crate) fn cipher_key_id(&self) -> CipherKeyId {
        match self {
            AesCipher::Aes128(_, id) => *id,
            AesCipher::Aes256(_, id) => *id,
        }
    }
    #[inline]
    pub(crate) fn encrypt(&self, nonce: &Nonce, plaintext: &[u8]) -> Result<Vec<u8>> {
        match self {
            AesCipher::Aes128(ref cipher, _) => cipher.encrypt(nonce, plaintext).map_err(|_| MorsEncryptError::EncryptError {
                nonce: format!("{:?}", nonce),
                plaintext: format!("{:?}", plaintext),
            }),
            AesCipher::Aes256(ref cipher, _) => cipher.encrypt(nonce, plaintext).map_err(|_| MorsEncryptError::EncryptError {
                nonce: format!("{:?}", nonce),
                plaintext: format!("{:?}", plaintext),
            })
        }
    }
    #[inline]
    pub(crate) fn encrypt_with_slice(&self, nonce: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        self.encrypt(Nonce::from_slice(nonce), plaintext)
    }
    #[inline]
    pub(crate) fn decrypt(&self, nonce: &Nonce, ciphertext: &[u8]) -> Result<Vec<u8>> {
        match self {
            AesCipher::Aes128(ref cipher, _) => cipher.decrypt(nonce, ciphertext).map_err(|_| MorsEncryptError::DecryptError {
                nonce: format!("{:?}", nonce),
                ciphertext: format!("{:?}", ciphertext),
            }),
            AesCipher::Aes256(ref cipher, _) => cipher.decrypt(nonce, ciphertext).map_err(|_| MorsEncryptError::DecryptError {
                nonce: format!("{:?}", nonce),
                ciphertext: format!("{:?}", ciphertext),
            })
        }
    }
    #[inline]
    pub(crate) fn decrypt_with_slice(&self, nonce: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>> {
        self.decrypt(Nonce::from_slice(nonce), ciphertext)
    }
    #[inline]
    pub(crate) fn generate_key(&self) -> Vec<u8> {
        match self {
            AesCipher::Aes128(_, _) => Aes128Gcm::generate_key(&mut OsRng).to_vec(),
            AesCipher::Aes256(_, _) => Aes256Gcm::generate_key(&mut OsRng).to_vec(),
        }
    }
    #[inline]
    pub(crate) fn generate_nonce() -> Nonce {
        Aes128Gcm::generate_nonce(&mut OsRng)
    }
}
