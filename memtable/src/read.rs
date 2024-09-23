use crate::memtable::Memtable;
use crate::Result;
use mors_common::{
    kv::ValueMeta,
    ts::{KeyTs, KeyTsBorrow, TxnTs},
};
use mors_traits::memtable::MemtableTrait;
use mors_traits::{file::StorageTrait, kms::Kms, skip_list::SkipListTrait};
impl<T: SkipListTrait, K: Kms, S: StorageTrait> Memtable<T, K, S> {
    pub fn get_impl(
        &self,
        key: &KeyTs,
    ) -> Result<Option<(TxnTs, Option<ValueMeta>)>> {
        let v = self
            .skip_list()
            .get_key_value(&key.encode(), true)?
            .and_then(|(k, v)| {
                let k: KeyTsBorrow = k.into();
                if k.key() == key.key() {
                    Some((k.txn_ts(), v.and_then(ValueMeta::decode)))
                } else {
                    None
                }
            });
        Ok(v)
    }
}
