use std::fmt::Debug;

use aead::consts::U12;
use aead::generic_array::GenericArray;
use aead::{Aead, AeadCore, KeyInit, OsRng};
#[cfg(feature = "aes-gcm")]
use aes_gcm::{Aes128Gcm, Aes256Gcm};
#[cfg(feature = "aes-gcm-siv")]
use aes_gcm_siv::Aes128GcmSiv as Aes128Gcm;
#[cfg(feature = "aes-gcm-siv")]
use aes_gcm_siv::Aes256GcmSiv as Aes256Gcm;
use mors_traits::kms::{CipherKeyId, KmsCipher};

use crate::error::MorsEncryptError;

pub type Nonce = GenericArray<u8, U12>;
type Result<T> = std::result::Result<T, MorsEncryptError>;

pub enum AesCipher {
    Aes128(Aes128Gcm, CipherKeyId),
    Aes256(Aes256Gcm, CipherKeyId),
}
impl KmsCipher for AesCipher {
    type ErrorType = MorsEncryptError;

    fn cipher_key_id(&self) -> CipherKeyId {
        todo!()
    }

    fn generate_nonce() -> Vec<u8> {
        todo!()
    }

    fn decrypt_with_slice(
        &self,
        nonce: &[u8],
        ciphertext: &[u8],
    ) -> std::result::Result<Vec<u8>, Self::ErrorType> {
        self.decrypt(Nonce::from_slice(nonce), ciphertext)
    }

    fn encrypt_with_slice(
        &self,
        nonce: &[u8],
        plaintext: &[u8],
    ) -> std::result::Result<Vec<u8>, Self::ErrorType> {
        self.encrypt(Nonce::from_slice(nonce), plaintext)
    }
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
            Self::Aes128(_, id) => {
                f.debug_tuple("Aes128Siv:").field(id).finish()
            }
            Self::Aes256(_, id) => {
                f.debug_tuple("Aes256Siv:").field(id).finish()
            }
        }
    }
}

impl AesCipher {
    #[inline]
    pub fn new(key: &[u8], id: CipherKeyId) -> Result<Self> {
        let cipher = match key.len() {
            16 => Self::Aes128(Aes128Gcm::new_from_slice(key).unwrap(), id),
            32 => Self::Aes256(Aes256Gcm::new_from_slice(key).unwrap(), id),
            _ => return Err(MorsEncryptError::InvalidEncryptionKey),
        };
        Ok(cipher)
    }
    pub fn cipher_key_id(&self) -> CipherKeyId {
        match self {
            AesCipher::Aes128(_, id) => *id,
            AesCipher::Aes256(_, id) => *id,
        }
    }
    #[inline]
    pub fn encrypt(&self, nonce: &Nonce, plaintext: &[u8]) -> Result<Vec<u8>> {
        match self {
            AesCipher::Aes128(ref cipher, _) => cipher
                .encrypt(nonce, plaintext)
                .map_err(|_| MorsEncryptError::EncryptError {
                    nonce: format!("{:?}", nonce),
                    plaintext: format!("{:?}", plaintext),
                }),
            AesCipher::Aes256(ref cipher, _) => cipher
                .encrypt(nonce, plaintext)
                .map_err(|_| MorsEncryptError::EncryptError {
                    nonce: format!("{:?}", nonce),
                    plaintext: format!("{:?}", plaintext),
                }),
        }
    }
    #[inline]
    pub fn decrypt(&self, nonce: &Nonce, ciphertext: &[u8]) -> Result<Vec<u8>> {
        match self {
            AesCipher::Aes128(ref cipher, _) => cipher
                .decrypt(nonce, ciphertext)
                .map_err(|_| MorsEncryptError::DecryptError {
                    nonce: format!("{:?}", nonce),
                    ciphertext: format!("{:?}", ciphertext),
                }),
            AesCipher::Aes256(ref cipher, _) => cipher
                .decrypt(nonce, ciphertext)
                .map_err(|_| MorsEncryptError::DecryptError {
                    nonce: format!("{:?}", nonce),
                    ciphertext: format!("{:?}", ciphertext),
                }),
        }
    }
    #[inline]
    pub fn generate_key(&self) -> Vec<u8> {
        match self {
            AesCipher::Aes128(_, _) => {
                Aes128Gcm::generate_key(&mut OsRng).to_vec()
            }
            AesCipher::Aes256(_, _) => {
                Aes256Gcm::generate_key(&mut OsRng).to_vec()
            }
        }
    }
    #[inline]
    pub fn generate_nonce() -> Nonce {
        Aes128Gcm::generate_nonce(&mut OsRng).into()
    }
}
