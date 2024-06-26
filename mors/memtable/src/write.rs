use mors_traits::file_id::MemtableId;
use mors_traits::kms::{Kms, KmsCipher};
use mors_traits::kv::{Entry, Meta};
use mors_traits::memtable::Memtable;
use mors_traits::skip_list::SkipList;
use mors_traits::ts::KeyTs;

use mors_wal::error::MorsWalError;
use mors_wal::read::LogFileIter;
use mors_wal::LogFile;

use crate::error::MorsMemtableError;
use crate::memtable::{MorsMemtable, MorsMemtableBuilder};
use crate::Result;

impl<T: SkipList, K: Kms> MorsMemtable<T, K>
where
    MorsMemtableError: From<<T as SkipList>::ErrorType>,
    MorsWalError: From<<K as Kms>::ErrorType>
        + From<<<K as Kms>::Cipher as KmsCipher>::ErrorType>,
{
    pub(crate) fn reload(&mut self) -> Result<()> {
        let mut wal_iter = LogFileIter::<MemtableId, K>::new(
            &self.wal,
            LogFile::<MemtableId, K>::LOG_HEADER_SIZE,
        );

        while let Some(next) = wal_iter.next()? {
            for (entry, _vptr) in next {
                self.max_version = self.max_version.max(entry.version());
                self.skip_list.push(
                    &entry.key_ts().encode(),
                    &entry.value_meta().encode(),
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
            &entry.key_ts().encode(),
            &entry.value_meta().encode(),
        )?;
        self.max_version = self.max_version.max(entry.version());
        Ok(())
    }
}
impl<T: SkipList, K: Kms> Memtable<K> for MorsMemtable<T, K>
where
    MorsMemtableError: From<<T as SkipList>::ErrorType>,
    MorsWalError: From<<K as Kms>::ErrorType>
        + From<<<K as Kms>::Cipher as KmsCipher>::ErrorType>,
{
    type ErrorType = MorsMemtableError;
    type MemtableBuilder = MorsMemtableBuilder<T>;

    fn push(&mut self, entry: &mors_traits::kv::Entry) -> Result<()> {
        self.wal.write_entry(&mut self.buf, entry)?;
        if entry.meta().contains(Meta::FIN_TXN) {
            return Ok(());
        }
        self.skip_list.push(
            &entry.key_ts().encode(),
            &entry.value_meta().encode(),
        )?;
        self.max_version = self.max_version.max(entry.version());
        Ok(())
    }

    fn size(&self) -> usize {
        todo!()
    }

    fn get(
        &self,
        key_ts: &KeyTs,
    ) -> Option<(mors_traits::ts::TxnTs, mors_traits::kv::ValueMeta)> {
        todo!()
    }
}
