use bytes::BufMut;
use mors_common::{file_id::FileId, kv::Entry};
use mors_traits::kms::Kms;
use std::{
    hash::Hasher,
    io::{self, Write},
};

use crate::{error::MorsWalError, header::LogEntryHeader, LogFile, Result};
impl<F: FileId, K: Kms> LogFile<F, K> {
    pub fn set_len(&mut self, end_offset: usize) -> io::Result<()> {
        let file_size = self.mmap.file_len()? as usize;
        if end_offset == file_size {
            return Ok(());
        }
        self.set_size(end_offset);
        self.mmap.set_len(end_offset)?;
        Ok(())
    }
    pub fn write_entry(
        &mut self,
        buf: &mut Vec<u8>,
        entry: &Entry,
    ) -> Result<()> {
        buf.clear();
        let buf = self.encode_entry(entry)?;
        self.mmap.write_all(&buf)?;
        Ok(())
    }
    pub fn append_entry(&self, entry: &Entry) -> Result<usize> {
        let encode = self.encode_entry(entry)?;
        let write_at = self
            .append_pos()
            .fetch_add(encode.len(), std::sync::atomic::Ordering::Relaxed);
        if let Err(e) = self.mmap.append(write_at, &encode) {
            if e.kind() == io::ErrorKind::Other {
                return Err(MorsWalError::StorageFull);
            }
        };
        Ok(encode.len())
    }
    pub fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.mmap.write_all(buf)
    }
    pub fn flush(&self) -> Result<()> {
        Ok(self.mmap.flush_range(
            0,
            self.append_pos().load(std::sync::atomic::Ordering::SeqCst),
        )?)
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
