use std::sync::atomic::Ordering;

use mors_common::{
    file_id::MemtableId,
    kv::{Entry, Meta},
};
use mors_traits::{kms::Kms, skip_list::SkipListTrait};
use mors_wal::{read::LogFileIter, LogFile};

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
                self.max_txn_ts
                    .fetch_max(entry.version().to_u64(), Ordering::Relaxed);
                // self.max_version = self.max_version.max(entry.version());
                self.skip_list.push(
                    &entry.key_ts().encode(),
                    &entry.value_meta().encode(),
                )?;
            }
        }

        let end_offset = wal_iter.valid_end_offset();
        if end_offset < self.wal.len() && self.read_only {
            return Err(MorsMemtableError::TruncateNeeded(
                end_offset,
                self.wal.len(),
            ));
        }

        self.wal.set_len(end_offset)?;
        Ok(())
    }
    pub fn push_impl(&self, entry: &Entry) -> Result<()> {
        self.wal.append_entry(entry)?;
        if entry.meta().contains(Meta::FIN_TXN) {
            return Ok(());
        }
        self.skip_list
            .push(&entry.key_ts().encode(), &entry.value_meta().encode())?;
        // let txn_ts = entry.version().to_u64();
        self.max_txn_ts
            .fetch_max(entry.version().to_u64(), Ordering::Relaxed);
        // self.max_version = self.max_version.max(entry.version());
        Ok(())
    }
    pub fn flush_impl(&self) -> Result<()> {
        self.wal.flush()?;
        Ok(())
    }
}
