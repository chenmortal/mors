use mors_traits::file_id::MemtableId;
use mors_traits::kv::{Entry, Meta};
use mors_traits::skip_list::SkipList;
use mors_wal::LogFile;
use mors_wal::read::LogFileIter;

use crate::{MemtableId, Result};
use crate::error::MorsMemtableError;
use crate::memtable::MorsMemtable;

impl<T: SkipList> MorsMemtable<T>
where
    MorsMemtableError: From<<T as SkipList>::ErrorType>,
{
    pub(crate) fn reload(&mut self) -> Result<()> {
        let mut wal_iter = LogFileIter::<MemtableId>::new(
            &self.wal,
            LogFile::<MemtableId>::LOG_HEADER_SIZE,
        )?;

        while let Some(next) = wal_iter.next()? {
            for (entry, _vptr) in next {
                self.max_version = self.max_version.max(entry.version());
                self.skip_list.push(
                    &entry.key_ts().serialize(),
                    &entry.value_meta().serialize(),
                )?;
            }
        }

        let end_offset = wal_iter.valid_end_offset();
        if end_offset < self.wal.size() && self.read_only {
            return Err(MorsMemtableError::TruncateNeeded(
                end_offset,
                self.wal.size(),
            ));
        }

        self.wal.truncate(end_offset)?;
        Ok(())
    }
    pub fn push(&mut self, entry: &Entry) -> Result<()> {
        self.wal.write_entry(&mut self.buf, entry)?;
        if entry.meta().contains(Meta::FIN_TXN) {
            return Ok(());
        }
        self.skip_list.push(
            &entry.key_ts().serialize(),
            &entry.value_meta().serialize(),
        )?;
        self.max_version = self.max_version.max(entry.version());
        Ok(())
    }
}
