use mors_traits::{
    kv::Meta, memtable::Memtable, skip_list::SkipList, ts::KeyTs,
};

use crate::{error::MorsMemtableError, memtable::MorsMemtable};

impl<T: SkipList> Memtable for MorsMemtable<T>
where
    MorsMemtableError: From<<T as SkipList>::ErrorType>,
{
    type ErrorType = MorsMemtableError;

    fn push(
        &mut self,
        entry: &mors_traits::kv::Entry,
    ) -> Result<(), Self::ErrorType> {
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
