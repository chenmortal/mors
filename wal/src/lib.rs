use crate::error::MorsWalError;
use bytes::{Buf, BufMut};
use mors_common::file_id::FileId;
use mors_traits::file::{StorageBuilderTrait, StorageTrait};
use mors_traits::kms::{CipherKeyId, Kms, KmsCipher};
use std::{
    fs::remove_file,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

pub mod error;
pub mod header;
pub mod read;
pub mod storage;
pub mod write;
type Result<T> = std::result::Result<T, MorsWalError>;
pub struct LogFile<F: FileId, K: Kms, S: StorageTrait> {
    id: F,
    kms: K,
    cipher: Option<K::Cipher>,
    storage: S,
    size: AtomicUsize,
    path_buf: PathBuf,
    base_nonce: Vec<u8>,
    valid_len: AtomicU64,
}
impl<F: FileId, K: Kms, S: StorageTrait> LogFile<F, K, S> {
    pub fn id(&self) -> F {
        self.id
    }
}
impl<F: FileId, K: Kms, S: StorageTrait> LogFile<F, K, S> {
    pub fn open<P: AsRef<Path>>(
        id: F,
        path_buf: P,
        max_size: u64,
        builder: S::StorageBuilder,
        kms: K,
    ) -> Result<Self> {
        let is_exist = path_buf.as_ref().exists();
        let mmap = builder.build(&path_buf, max_size)?;
        let mut log_file = Self {
            id,
            kms,
            cipher: None,
            storage: mmap,
            path_buf: path_buf.as_ref().to_owned(),
            size: AtomicUsize::new(0),
            base_nonce: Vec::new(),

            valid_len: AtomicU64::new(max_size),
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
        log_file
            .size
            .store(log_file.storage.file_len()? as usize, Ordering::Relaxed);

        let mut buf = vec![0; Self::LOG_HEADER_SIZE];
        if log_file.storage.read(&mut buf)? != Self::LOG_HEADER_SIZE {
            return Err(MorsWalError::InvalidLogHeader(
                path_buf.as_ref().to_owned(),
            ));
        };

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
        debug_assert_eq!(
            self.storage.append(&buf, Ordering::Relaxed)?,
            Self::LOG_HEADER_SIZE
        );
        self.storage.flush_range(0, Self::LOG_HEADER_SIZE)?;
        Ok(())
    }
    fn cipher_key_id(&self) -> CipherKeyId {
        self.cipher
            .as_ref()
            .map(|c| c.cipher_key_id())
            .unwrap_or_default()
    }
    // #[inline]
    // fn generate_nonce(&self, offset: usize) -> Vec<u8> {
    //     let mut v = Vec::with_capacity(K::Cipher::NONCE_SIZE);
    //     let offset = offset.to_ne_bytes();
    //     v.extend_from_slice(
    //         &self.base_nonce[..K::Cipher::NONCE_SIZE - offset.len()],
    //     );
    //     v.extend_from_slice(&offset);
    //     v
    // }
    // fn decrypt(&self, buf: &[u8]) -> Result<Option<Vec<u8>>> {
    //     Ok(match self.cipher.as_ref() {
    //         Some(c) => Some(c.decrypt_with_slice(&self.base_nonce, buf)?),
    //         None => None,
    //     })
    // }
    fn encrypt(&self, buf: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(match self.cipher.as_ref() {
            Some(c) => {
                // let nonce = self.generate_nonce(offset);
                Some(c.encrypt_with_slice(&self.base_nonce, buf)?)
            }
            None => None,
        })
    }
    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
    // pub fn max_size(&self) -> usize {
    //     self.storage.len().unwrap_or(0)
    // }
    pub fn is_empty(&self) -> bool {
        self.storage.load_append_pos(Ordering::Relaxed) == Self::LOG_HEADER_SIZE
    }
    pub fn delete(&self) -> Result<()> {
        Ok(self.storage.delete()?)
    }
    pub(crate) fn set_size(&self, size: usize) {
        self.size.store(size, Ordering::Relaxed);
    }
    pub fn set_valid_len(&self, valid_len: u64) {
        self.valid_len.store(valid_len, Ordering::SeqCst);
    }
}
impl<F: FileId, K: Kms, S: StorageTrait> Drop for LogFile<F, K, S> {
    fn drop(&mut self) {
        let valid_size = self.storage.load_append_pos(Ordering::Relaxed);
        if let Err(e) = self.flush() {
            eprintln!("Error: {:?}", e);
        };
        if let Err(e) = self.storage.set_len(valid_size as u64) {
            eprintln!("Error: {:?}", e);
        };

        // let valid_len = self.valid_len.load(Ordering::SeqCst);
        // self.mmap.set_len(valid_len as usize).unwrap();
    }
}
