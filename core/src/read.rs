use mors_common::{
    kv::ValueMeta,
    ts::{KeyTs, TxnTs},
};
use mors_traits::{
    kms::Kms, levelctl::LevelCtlTrait, memtable::MemtableTrait,
    skip_list::SkipListTrait, sstable::TableTrait, 
    vlog::VlogCtlTrait,
};

use crate::core::CoreInner;
use crate::Result;
impl<M, K, L, T, S, V> CoreInner<M, K, L, T, S, V>
where
    M: MemtableTrait<S, K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
    S: SkipListTrait,
    V: VlogCtlTrait<K>,

{
    pub(crate) async fn get(
        &self,
        key: &KeyTs,
    ) -> Result<Option<(TxnTs, Option<ValueMeta>)>> {
        let mut max_txn_ts = TxnTs::default();
        let mut max_value = None;

        if let Some(mem) = self.read_memtable()? {
            if let Some((txn_ts, value)) = mem.get(key)? {
                if txn_ts == key.txn_ts() {
                    return Ok(Some((txn_ts, value)));
                }
                if txn_ts > max_txn_ts {
                    max_txn_ts = txn_ts;
                    max_value = value;
                }
            };
        }
        {
            let immut_r = self.immut_memtable().read()?;
            for mem in immut_r.iter() {
                if let Some((txn_ts, value)) = mem.get(key)? {
                    if txn_ts == key.txn_ts() {
                        return Ok(Some((txn_ts, value)));
                    }
                    if txn_ts > max_txn_ts {
                        max_txn_ts = txn_ts;
                        max_value = value;
                    }
                };
            }
        }
        if let Some((txn_ts, value)) = self.levelctl().get(key).await? {
            if txn_ts == key.txn_ts() {
                return Ok(Some((txn_ts, value)));
            }
            if txn_ts > max_txn_ts {
                max_txn_ts = txn_ts;
                max_value = value;
            }
        };
        if max_txn_ts != TxnTs::default() {
            return Ok(Some((max_txn_ts, max_value)));
        }
        Ok(None)
    }
}
