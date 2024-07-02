use mors_traits::file_id::MemtableId;
use mors_traits::kms::{Kms, KmsCipher};
use mors_traits::kv::{Entry, Meta};
use mors_traits::memtable::MemtableTrait;
use mors_traits::skip_list::SkipListTrait;
use mors_traits::ts::KeyTs;

use mors_wal::error::MorsWalError;
use mors_wal::read::LogFileIter;
use mors_wal::LogFile;

use crate::error::MemtableError;
use crate::memtable::{Memtable, MemtableBuilder};
use crate::Result;

impl<T: SkipListTrait, K: Kms> Memtable<T, K>
where
    MemtableError: From<<T as SkipListTrait>::ErrorType>,
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
            return Err(MemtableError::TruncateNeeded(
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
impl<T: SkipListTrait, K: Kms> MemtableTrait<K> for Memtable<T, K>
where
    MemtableError: From<<T as SkipListTrait>::ErrorType>,
    MorsWalError: From<<K as Kms>::ErrorType>
        + From<<<K as Kms>::Cipher as KmsCipher>::ErrorType>,
{
    type ErrorType = MemtableError;
    type MemtableBuilder = MemtableBuilder<T>;

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
