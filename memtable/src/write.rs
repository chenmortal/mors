use mors_traits::file_id::MemtableId;
use mors_traits::kms::Kms;
use mors_traits::kv::{Entry, Meta};
use mors_traits::skip_list::SkipListTrait;
use mors_wal::read::LogFileIter;
use mors_wal::LogFile;

use crate::error::MorsMemtableError;
use crate::memtable::Memtable;
use crate::Result;

impl<T: SkipListTrait, K: Kms> Memtable<T, K> {
    pub(crate) fn reload(&mut self) -> Result<()> {
        let mut wal_iter = LogFileIter::<MemtableId, K>::new(
            &self.wal,
            LogFile::<MemtableId, K>::LOG_HEADER_SIZE,
        );

        while let Some(next) = wal_iter.next_entry()? {
            for (entry, _vptr) in next {
                self.max_version = self.max_version.max(entry.version());
                self.skip_list.push(
                    &entry.key_ts().encode(),
                    &entry.value_meta().encode(),
                )?;
            }
        }

        let end_offset = wal_iter.valid_end_offset();
        if end_offset < self.wal.max_size() && self.read_only {
            return Err(MorsMemtableError::TruncateNeeded(
                end_offset,
                self.wal.max_size(),
            ));
        }

        self.wal.truncate(end_offset)?;
        Ok(())
    }
    pub fn push_impl(&mut self, entry: &Entry) -> Result<()> {
        self.wal.write_entry(&mut self.buf, entry)?;
        if entry.meta().contains(Meta::FIN_TXN) {
            return Ok(());
        }
        self.skip_list
            .push(&entry.key_ts().encode(), &entry.value_meta().encode())?;
        self.max_version = self.max_version.max(entry.version());
        Ok(())
    }
}
