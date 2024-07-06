use std::fs::remove_file;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::{Buf, BufMut};

use mors_common::mmap::{MmapFile, MmapFileBuilder};
// use mors_encrypt::cipher::AesCipher;
// use mors_encrypt::registry::MorsKms;

// use mors_encrypt::NONCE_SIZE;
use mors_traits::file_id::FileId;
use mors_traits::kms::{CipherKeyId, Kms, KmsCipher};

use crate::error::MorsWalError;

pub mod error;
pub mod read;
pub mod write;

type Result<T> = std::result::Result<T, MorsWalError>;
pub struct LogFile<F: FileId, K: Kms> {
    id: F,
    kms: K,
    cipher: Option<K::Cipher>,
    mmap: MmapFile,
    size: AtomicUsize,
    path_buf: PathBuf,
    base_nonce: Vec<u8>,
}
impl<F: FileId, K: Kms> LogFile<F, K> {
    pub fn id(&self) -> F {
        self.id
    }
}
impl<F: FileId, K: Kms> LogFile<F, K>
// where
    // MorsWalError: From<<K::Cipher as KmsCipher>::ErrorType>,
{
    pub fn open(
        id: F,
        path_buf: PathBuf,
        max_size: u64,
        builder: MmapFileBuilder,
        kms: K,
    ) -> Result<Self> {
        let is_exist = Path::new(path_buf.as_path()).exists();
        let mmap = builder.build(&path_buf, max_size)?;
        let mut log_file = Self {
            id,
            kms,
            cipher: None,
            mmap,
            path_buf,
            size: AtomicUsize::new(0),
            base_nonce: Vec::new(),
        };

        if !is_exist {
            let result = log_file.bootstrap();
            if result.is_err() {
                remove_file(&log_file.path_buf)?;
                result?;
            }
            log_file
                .size
                .store(Self::LOG_HEADER_SIZE, Ordering::Relaxed);
        }
        log_file.size.store(log_file.mmap.len()?, Ordering::Relaxed);

        let mut buf = Vec::with_capacity(Self::LOG_HEADER_SIZE);
        log_file.mmap.read_exact(&mut buf)?;
        debug_assert_eq!(buf.len(), Self::LOG_HEADER_SIZE);
        let mut buf_ref = buf.as_slice();
        let key_id: CipherKeyId = buf_ref.get_u64().into();
        log_file.cipher = log_file.kms.get_cipher(key_id)?;

        debug_assert_eq!(buf_ref.len(), 12);
        log_file.base_nonce = buf_ref.to_vec();

        Ok(log_file)
    }
    // bootstrap will initialize the log file with key id and baseIV.
    // The below figure shows the layout of log file.
    // +----------------+------------------+------------------+
    // | keyID(8 bytes) |  baseIV(12 bytes)|	 entry...     |
    // +----------------+------------------+------------------+
    pub const LOG_HEADER_SIZE: usize = 20;

    fn bootstrap(&mut self) -> Result<()> {
        self.cipher = self.kms.latest_cipher()?;
        self.base_nonce = K::Cipher::generate_nonce();

        let mut buf = Vec::with_capacity(Self::LOG_HEADER_SIZE);
        buf.put_u64(self.cipher_key_id().into());
        buf.put(self.base_nonce.as_ref());

        debug_assert_eq!(buf.len(), Self::LOG_HEADER_SIZE);
        debug_assert_eq!(self.mmap.write(&buf)?, Self::LOG_HEADER_SIZE);
        self.mmap.flush()?;
        Ok(())
    }
    fn cipher_key_id(&self) -> CipherKeyId {
        self.cipher
            .as_ref()
            .map(|c| c.cipher_key_id())
            .unwrap_or_default()
    }
    #[inline]
    fn generate_nonce(&self, offset: usize) -> Vec<u8> {
        let mut v = Vec::with_capacity(K::NONCE_SIZE);
        let offset = offset.to_ne_bytes();
        v.extend_from_slice(&self.base_nonce[..K::NONCE_SIZE - offset.len()]);
        v.extend_from_slice(&offset);
        v
    }
    fn decrypt(&self, buf: &[u8], offset: usize) -> Result<Option<Vec<u8>>> {
        Ok(match self.cipher.as_ref() {
            Some(c) => {
                let nonce = self.generate_nonce(offset);
                Some(c.decrypt_with_slice(&nonce, buf)?)
            }
            None => None,
        })
    }
    fn encrypt(&self, buf: &[u8], offset: usize) -> Result<Option<Vec<u8>>> {
        Ok(match self.cipher.as_ref() {
            Some(c) => {
                let nonce = self.generate_nonce(offset);
                Some(c.encrypt_with_slice(&nonce, buf)?)
            }
            None => None,
        })
    }
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
    pub(crate) fn set_size(&self, size: usize) {
        self.size.store(size, Ordering::Relaxed);
    }
}
