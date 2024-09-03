use log::error;
use mors_common::{
    kv::ValueMeta,
    ts::{KeyTs, KeyTsBorrow, TxnTs},
};
use mors_traits::{
    iter::{KvCacheIter, KvSeekIter},
    kms::Kms,
    levelctl::{Level, LEVEL0},
    sstable::TableTrait,
};

use crate::ctl::LevelCtl;
use crate::error::MorsLevelCtlError;
use crate::handler::LevelHandler;
type Result<T> = std::result::Result<T, MorsLevelCtlError>;
impl<T: TableTrait<K::Cipher>, K: Kms> LevelCtl<T, K> {
    pub(crate) async fn get_impl(
        &self,
        key: &KeyTs,
    ) -> Result<Option<(TxnTs, Option<ValueMeta>)>> {
        let mut max_txn = TxnTs::default();
        let mut max_value = None;
        for level in 0..=self.max_level().to_u8() {
            let level: Level = level.into();
            let handler = self.handler(level).unwrap();
            if let Some((txn, value)) = handler.get(key).await? {
                if txn == key.txn_ts() {
                    return Ok(Some((txn, value)));
                }
                if txn > max_txn {
                    max_txn = txn;
                    max_value = value;
                }
            };
        }
        if max_txn != TxnTs::default() {
            return Ok(Some((max_txn, max_value)));
        }
        Ok(None)
    }
}
impl<T: TableTrait<K::Cipher>, K: Kms> LevelHandler<T, K> {
    async fn get(
        &self,
        key: &KeyTs,
    ) -> Result<Option<(TxnTs, Option<ValueMeta>)>> {
        if let Some(tables) = self.seek_table(key) {
            let mut tasks = Vec::with_capacity(tables.len());
            for table in tables {
                let ks = key.encode();
                tasks.push(tokio::spawn(async move {
                    let k = KeyTsBorrow::from(ks.as_ref());
                    let mut iter = table.iter(true);
                    match iter.seek(k) {
                        Ok(seek) => {
                            if seek {
                                if let Some(seek_key) = iter.key() {
                                    if k.key() == seek_key.key() {
                                        let txn = seek_key.txn_ts();
                                        return Some((txn, iter.value()));
                                    }
                                }
                            }
                            None
                        }
                        Err(e) => {
                            error!("{} seek  error:{}", table.id(), e);
                            None
                        }
                    }
                }));
            }
            let mut max_txn = TxnTs::default();
            let mut max_value = None;
            for task in tasks {
                if let Some((txn, value)) = task.await? {
                    if txn > max_txn {
                        max_txn = txn;
                        max_value = value;
                    }
                };
            }
            if max_txn != TxnTs::default() {
                return Ok(Some((max_txn, max_value)));
            }
        };
        Ok(None)
    }
    fn seek_table(&self, key: &KeyTs) -> Option<Vec<T>> {
        let handler = self.read();
        if self.level() == LEVEL0 {
            handler
                .tables()
                .iter()
                .rev()
                .filter(|t| t.may_contain(key.key()))
                .cloned()
                .collect::<Vec<_>>()
                .into()
        } else if let Ok(table_index) =
            handler.tables().binary_search_by(|t| t.biggest().cmp(key))
        {
            if table_index >= handler.tables().len() {
                return None;
            }
            let t = handler.tables()[table_index].clone();
            if !t.may_contain(key.key()) {
                return None;
            }
            vec![t].into()
        } else {
            None
        }
    }
}
