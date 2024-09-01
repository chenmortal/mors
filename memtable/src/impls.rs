use std::{collections::VecDeque, sync::Arc};

use mors_common::{
    file_id::MemtableId,
    kv::{Entry, ValueMeta},
    ts::{KeyTs, TxnTs},
};
use mors_traits::{
    kms::Kms,
    memtable::{MemtableBuilderTrait, MemtableError, MemtableTrait},
    skip_list::SkipListTrait,
};

use crate::{
    error::MorsMemtableError,
    memtable::{Memtable, MemtableBuilder},
};

type Result<T> = std::result::Result<T, MemtableError>;
impl<T: SkipListTrait, K: Kms> MemtableBuilderTrait<Memtable<T, K>, T, K>
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

    fn set_num_memtables(&mut self, num_memtables: usize) {
        self.set_num_memtables_impl(num_memtables);
    }

    fn set_memtable_size(&mut self, memtable_size: usize) {
        self.set_memtable_size_impl(memtable_size);
    }
    
    fn max_batch_size(&self) -> usize {
        todo!()
    }
    
    fn max_batch_count(&self) -> usize {
        todo!()
    }
}
impl<T: SkipListTrait, K: Kms> MemtableTrait<T, K> for Memtable<T, K> {
    type ErrorType = MorsMemtableError;
    type MemtableBuilder = MemtableBuilder<T>;

    fn push(&self, entry: &Entry) -> Result<()> {
        Ok(self.push_impl(entry)?)
    }

    fn size(&self) -> usize {
        self.skip_list.size()
    }

    fn get(&self, key: &KeyTs) -> Result<Option<(TxnTs, Option<ValueMeta>)>> {
        Ok(self.get_impl(key)?)
    }

    fn max_version(&self) -> TxnTs {
        self.max_txn_ts
            .load(std::sync::atomic::Ordering::SeqCst)
            .into()
    }

    fn is_full(&self) -> bool {
        self.size() >= self.memtable_size
    }

    fn id(&self) -> MemtableId {
        self.wal.id()
    }

    fn skip_list(&self) -> T {
        self.skip_list.clone()
    }

    fn flush(&self) -> std::result::Result<(), MemtableError> {
        Ok(self.flush_impl()?)
    }

    fn delete_wal(&self) -> std::result::Result<(), MemtableError> {
        Ok(self.wal.delete().map_err(MorsMemtableError::Wal)?)
    }
    
}
