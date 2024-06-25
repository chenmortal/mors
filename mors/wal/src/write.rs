use std::{
    hash::Hasher,
    io::{self, Write},
    mem,
};

use bytes::BufMut;
use mors_traits::{file_id::FileId, kms::{Kms, KmsCipher}, kv::Entry, log_header::LogEntryHeader};

use crate::{error::MorsWalError, LogFile};
use crate::Result;
impl<F: FileId,K:Kms> LogFile<F,K> 
where
    MorsWalError:
        From<<K as Kms>::ErrorType> + From<<K::Cipher as KmsCipher>::ErrorType>,
{
    pub fn truncate(&mut self, end_offset: usize) -> io::Result<()> {
        let file_size = self.mmap.file_len()? as usize;
        if end_offset == file_size {
            return Ok(());
        }
        self.set_size(end_offset);
        self.mmap.set_len(end_offset)?;
        Ok(())
    }
    pub fn write_entry(&mut self, buf: &mut Vec<u8>, entry: &Entry)->Result<()> {
        buf.clear();
       
        let offset = self.mmap.write_at();
        let size = self.encode_entry(buf, entry, offset)?;
        self.mmap.pwrite(&buf[..size], offset)?;
        Ok(())
    }
    fn encode_entry(
        &self,
        buf: &mut Vec<u8>,
        entry: &Entry,
        offset: usize,
    ) -> Result<usize> {
        let header = LogEntryHeader::new(entry);
        let mut hash_writer = HashWriter {
            writer: buf,
            hasher: crc32fast::Hasher::new(),
        };
        let header_encode = header.encode();
        let header_len = hash_writer.write(&header_encode)?;
        let mut kv_buf = entry.key_ts().serialize();
        kv_buf.extend_from_slice(entry.value_meta().value());

        kv_buf = self.encrypt(&kv_buf, offset)?.unwrap_or(kv_buf);
        let kv_len = hash_writer.write(&kv_buf)?;
        let crc = hash_writer.hasher.finalize();
        let buf = hash_writer.writer;
        buf.put_u32(crc);
        Ok(header_len + kv_len + mem::size_of::<u32>())
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
