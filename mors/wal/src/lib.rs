use std::fs::remove_file;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;

use bytes::BufMut;

use mors_common::mmap::{MmapFile, MmapFileBuilder};
use mors_encrypt::cipher::{AesCipher, CipherKeyId};
use mors_encrypt::registry::Kms;
use mors_traits::file_id::FileId;

use crate::error::MorsWalError;

mod error;
type Result<T> = std::result::Result<T, MorsWalError>;
pub struct LogFile<F: FileId> {
    id: F,
    kms: Kms,
    cipher: Option<AesCipher>,
    mmap: MmapFile,
    size: AtomicUsize,
    path_buf: PathBuf,
    base_nonce: Vec<u8>,
}
impl<F: FileId> LogFile<F> {
    pub fn open(
        id: F,
        path_buf: PathBuf,
        max_size: u64,
        builder: MmapFileBuilder,
        kms: Kms,
    ) -> Result<()> {
        let is_exist = Path::new(path_buf.as_path()).exists();
        let mmap = builder.create(&path_buf, max_size)?;
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
                remove_file(log_file.path_buf)?;
                result?;
            }
            log_file
                .size
                .store(Self::LOG_HEADER_SIZE, std::sync::atomic::Ordering::Relaxed);
        }

        Ok(())
    }
    // bootstrap will initialize the log file with key id and baseIV.
    // The below figure shows the layout of log file.
    // +----------------+------------------+------------------+
    // | keyID(8 bytes) |  baseIV(12 bytes)|	 entry...     |
    // +----------------+------------------+------------------+
    pub const LOG_HEADER_SIZE: usize = 20;

    fn bootstrap(&mut self) -> Result<()> {
        self.cipher = self.kms.latest_cipher()?;
        self.base_nonce = AesCipher::generate_nonce().to_vec();

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
}
