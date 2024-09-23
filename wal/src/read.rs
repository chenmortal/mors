use crate::{
    error::MorsWalError::{self},
    header::LogEntryHeader,
    LogFile, Result,
};
use bytes::Buf;
use mors_common::{
    file_id::FileId,
    kv::{Entry, Meta, ValuePointer},
    ts::TxnTs,
};
use mors_traits::{
    file::StorageTrait,
    kms::{Kms, KmsCipher},
};
use std::{
    hash::Hasher,
    io::{self, BufReader, Read},
};
pub struct LogFileIter<'a, F: FileId, K: Kms, S: StorageTrait> {
    // log_file: &'a LogFile<F, K, S>,
    cipher: &'a Option<K::Cipher>,
    base_nonce: &'a Vec<u8>,
    id: F,
    record_offset: usize,
    reader: BufReader<&'a mut S>,
    entries_vptrs: Vec<(Entry, ValuePointer)>,
    valid_end_offset: usize,
}
impl<'a, F: FileId, K: Kms, S: StorageTrait> LogFileIter<'a, F, K, S> {
    pub fn new(log_file: &'a mut LogFile<F, K, S>, offset: usize) -> Self {
        // let mut buf = [0u8; offset];
        // log_file.storage.read_exact(buf);
        log_file.storage.set_read_pos(offset);
        let p = &log_file.cipher;
        let id = log_file.id();
        let reader = BufReader::new(&mut log_file.storage);
        // let reader = BufReader::new(&log_file.storage.as_ref()[offset..]);

        Self {
            // log_file,
            record_offset: offset,
            reader,
            entries_vptrs: Vec::new(),
            valid_end_offset: offset,
            cipher: p,
            id,
            base_nonce: &log_file.base_nonce,
        }
    }

    pub fn read_entry(&mut self) -> Result<(Entry, ValuePointer)> {
        let mut hash_reader = HashReader {
            reader: &mut self.reader,
            hasher: crc32fast::Hasher::new(),
            len: 0,
        };

        let entry_header = LogEntryHeader::decode_from(&mut hash_reader)?;
        let header_len = hash_reader.len;
        entry_header.check_key_len()?;

        let key_len = entry_header.key_len() as usize;
        let value_len = entry_header.value_len() as usize;

        let mut kv_buf = vec![0; key_len + value_len];
        hash_reader.read_exact(&mut kv_buf)?;

        if kv_buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "kv len can't be zero",
            )
            .into());
        }

        let kv_buf = match self.cipher.as_ref() {
            Some(c) => c.decrypt_with_slice(self.base_nonce, &kv_buf)?,
            None => kv_buf,
        };
        // kv_buf = self.log_file.decrypt(&kv_buf)?.unwrap_or(kv_buf);

        let mut entry = Entry::from_log(
            &kv_buf[..key_len],
            &kv_buf[key_len..],
            self.record_offset,
        );
        let value_meta = entry.value_meta_mut();
        value_meta.set_meta(entry_header.meta());
        value_meta.set_user_meta(entry_header.user_meta());
        value_meta.set_expires_at(entry_header.expires_at());

        let hash = hash_reader.hasher.finalize();
        let mut crc_buf = 0_u32.to_be_bytes();
        hash_reader.reader.read_exact(&mut crc_buf)?;

        let crc = crc_buf.as_slice().get_u32();
        if hash != crc {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to checksum crc32",
            )
            .into());
        };

        let size = header_len + key_len + value_len + crc_buf.len();
        debug_assert!(size == hash_reader.len + 4);

        let v_ptr =
            ValuePointer::new(self.id, size as u32, self.record_offset as u64);
        self.record_offset += size;
        Ok((entry, v_ptr))
    }
    //
    pub fn next_entry(
        &mut self,
    ) -> Result<Option<&Vec<(Entry, ValuePointer)>>> {
        let mut last_commit = TxnTs::default();
        self.entries_vptrs.clear();
        loop {
            match self.read_entry() {
                Ok((entry, v_ptr)) => {
                    if entry.meta().contains(Meta::TXN) {
                        let txn_ts = entry.version();
                        if last_commit == TxnTs::default() {
                            last_commit = txn_ts;
                        }
                        if last_commit != txn_ts {
                            break;
                        }
                        self.entries_vptrs.push((entry, v_ptr));
                    } else if entry.meta().contains(Meta::FIN_TXN) {
                        let txn_ts = entry.version();
                        if last_commit != txn_ts {
                            break;
                        }
                        self.valid_end_offset = self.record_offset;
                        return Ok(Some(&self.entries_vptrs));
                    } else {
                        if last_commit != TxnTs::default() {
                            break;
                        }
                        self.entries_vptrs.push((entry, v_ptr));
                        self.valid_end_offset = self.record_offset;
                        return Ok(Some(&self.entries_vptrs));
                    }
                }
                Err(MorsWalError::IoError(io))
                    if io.kind() == io::ErrorKind::UnexpectedEof =>
                {
                    break
                }
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }
    //
    pub fn valid_end_offset(&self) -> usize {
        self.valid_end_offset
    }
}

pub struct HashReader<'a, B: Read, T: Hasher> {
    reader: &'a mut BufReader<B>,
    hasher: T,
    len: usize,
}

impl<B: Read, T: Hasher> Read for HashReader<'_, B, T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let size = self.reader.read(buf)?;
        self.len += size;
        self.hasher.write(&buf[..size]);
        Ok(size)
    }
}
