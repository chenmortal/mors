use std::{collections::VecDeque, sync::Arc};

use mors_traits::{
    file_id::MemtableId,
    kms::Kms,
    memtable::{MemtableBuilderTrait, MemtableError, MemtableTrait},
    skip_list::SkipListTrait,
    ts::KeyTs,
};

use crate::{
    error::MorsMemtableError,
    memtable::{Memtable, MemtableBuilder},
};

type Result<T> = std::result::Result<T, MemtableError>;
impl<T: SkipListTrait, K: Kms> MemtableBuilderTrait<Memtable<T, K>, K>
    for MemtableBuilder<T>
{
    fn open(&self, kms: K, id: MemtableId) -> Result<Memtable<T, K>> {
        Ok(self.open_impl(kms, id)?)
    }

    fn open_exist(&self, kms: K) -> Result<VecDeque<Arc<Memtable<T, K>>>> {
        Ok(self.open_exist_impl(kms)?)
    }

    fn build(&self, kms: K) -> Result<Memtable<T, K>> {
        Ok(self.build_impl(kms)?)
    }
}
impl<T: SkipListTrait, K: Kms> MemtableTrait<K> for Memtable<T, K> {
    type ErrorType = MorsMemtableError;
    type MemtableBuilder = MemtableBuilder<T>;

    fn push(&mut self, entry: &mors_traits::kv::Entry) -> Result<()> {
        Ok(self.push_impl(entry)?)
    }

    fn size(&self) -> usize {
        self.skip_list.size()
    }

    fn get(
        &self,
        key_ts: &KeyTs,
    ) -> Result<Option<(mors_traits::ts::TxnTs, mors_traits::kv::ValueMeta)>>
    {
        todo!()
    }

    fn max_version(&self) -> mors_traits::ts::TxnTs {
        self.max_version
    }

    fn is_full(&self) -> bool {
        self.size() >= self.memtable_size
    }
    
    fn id(&self) -> MemtableId {
        self.wal.id()
    }
}
