use mors_common::kv::ValueMeta;
use mors_common::ts::{KeyTs, KeyTsBorrow, TxnTs};
use mors_traits::memtable::MemtableTrait;
use mors_traits::{kms::Kms, skip_list::SkipListTrait};

use crate::memtable::Memtable;
use crate::Result;
impl<T: SkipListTrait, K: Kms> Memtable<T, K> {
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
