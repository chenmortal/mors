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
use mors_traits::kms::{CipherKeyId, EncryptError, KmsCipher};

use crate::error::MorsEncryptError;
use crate::NONCE_SIZE;

pub type Nonce = GenericArray<u8, U12>;
type Result<T> = std::result::Result<T, MorsEncryptError>;

pub enum AesCipher {
    Aes128(Box<Aes128Gcm>, CipherKeyId),
    Aes256(Box<Aes256Gcm>, CipherKeyId),
}
impl KmsCipher for AesCipher {
    type ErrorType = MorsEncryptError;

    fn cipher_key_id(&self) -> CipherKeyId {
        match self {
            AesCipher::Aes128(_, id) => *id,
            AesCipher::Aes256(_, id) => *id,
        }
    }

    fn generate_nonce() -> Vec<u8> {
        AesCipher::generate_nonce().to_vec()
    }
    const NONCE_SIZE: usize = 12;
    fn decrypt_with_slice(
        &self,
        nonce: &[u8],
        ciphertext: &[u8],
    ) -> std::result::Result<Vec<u8>, EncryptError> {
        Ok(self.decrypt(Nonce::from_slice(nonce), ciphertext)?)
    }

    fn encrypt_with_slice(
        &self,
        nonce: &[u8],
        plaintext: &[u8],
    ) -> std::result::Result<Vec<u8>, EncryptError> {
        Ok(self.encrypt(Nonce::from_slice(nonce), plaintext)?)
    }

    fn decrypt(
        &self,
        data: &[u8],
    ) -> std::result::Result<Vec<u8>, EncryptError> {
        let nonce = &data[data.len() - NONCE_SIZE..];
        let ciphertext = &data[..data.len() - NONCE_SIZE];
        self.decrypt_with_slice(nonce, ciphertext)
    }

    fn encrypt(
        &self,
        data: &[u8],
    ) -> std::result::Result<Vec<u8>, EncryptError> {
        let nonce = Self::generate_nonce();
        let mut ciphertext = self.encrypt_with_slice(nonce.as_slice(), data)?;
        ciphertext.extend_from_slice(nonce.as_slice());
        Ok(ciphertext)
    }
}
impl Clone for AesCipher {
    fn clone(&self) -> Self {
        match self {
            AesCipher::Aes128(cipher, id) => {
                AesCipher::Aes128(cipher.clone(), *id)
            }
            AesCipher::Aes256(cipher, id) => {
                AesCipher::Aes256(cipher.clone(), *id)
            }
        }
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
            16 => Self::Aes128(
                Box::new(Aes128Gcm::new_from_slice(key).unwrap()),
                id,
            ),
            32 => Self::Aes256(
                Box::new(Aes256Gcm::new_from_slice(key).unwrap()),
                id,
            ),
            _ => return Err(MorsEncryptError::InvalidEncryptionKey),
        };
        Ok(cipher)
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
    pub(crate) fn generate_nonce() -> Nonce {
        Aes128Gcm::generate_nonce(&mut OsRng)
    }
}
