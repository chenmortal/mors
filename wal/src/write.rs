use bytes::BufMut;
use mors_common::{file_id::FileId, kv::Entry};
use mors_traits::file::StorageTrait;
use mors_traits::kms::Kms;
use std::{
    hash::Hasher,
    io::{self, Write},
    sync::atomic::Ordering,
};

use crate::{error::MorsWalError, header::LogEntryHeader, LogFile, Result};
impl<F: FileId, K: Kms, S: StorageTrait> LogFile<F, K, S> {
    pub fn set_len(&mut self, end_offset: usize) -> io::Result<()> {
        let file_size = self.storage.file_len()? as usize;
        if end_offset == file_size {
            return Ok(());
        }
        self.set_size(end_offset);
        self.storage.set_len(end_offset as u64)?;
        Ok(())
    }
    pub fn append_entry(&self, entry: &Entry) -> Result<usize> {
        let encode = self.encode_entry(entry)?;
        if let Err(e) = self.storage.append(&encode, Ordering::Relaxed) {
            if e.kind() == io::ErrorKind::Other {
                return Err(MorsWalError::StorageFull);
            }
        };
        Ok(encode.len())
    }
    pub fn flush(&self) -> Result<()> {
        Ok(self
            .storage
            .flush_range(0, self.storage.load_append_pos(Ordering::Relaxed))?)
    }
    pub fn encode_entry(&self, entry: &Entry) -> Result<Vec<u8>> {
        let header = LogEntryHeader::new(entry);
        let header_encode = header.encode();

        let mut kv_buf = entry.key_ts().encode();
        kv_buf.extend_from_slice(entry.value_meta().value());

        let mut buf = Vec::with_capacity(
            header_encode.len() + kv_buf.len() + size_of::<u32>(),
        );
        let mut hash_writer = HashWriter {
            writer: &mut buf,
            hasher: crc32fast::Hasher::new(),
        };

        let header_len = hash_writer.write(&header_encode)?;

        kv_buf = self.encrypt(&kv_buf)?.unwrap_or(kv_buf);
        let kv_len = hash_writer.write(&kv_buf)?;
        let crc = hash_writer.hasher.finalize();

        buf.put_u32(crc);
        debug_assert_eq!(buf.len(), header_len + kv_len + size_of::<u32>());
        Ok(buf)
    }
}
pub(crate) struct HashWriter<'a, T: Hasher> {
    writer: &'a mut Vec<u8>,
    hasher: T,
}

impl<T: Hasher> Write for HashWriter<'_, T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.writer.put_slice(buf);
        self.hasher.write(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
