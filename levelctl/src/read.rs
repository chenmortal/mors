use log::error;
use mors_common::kv::ValueMeta;
use mors_common::ts::{KeyTs, KeyTsBorrow, TxnTs};
use mors_traits::iter::{KvCacheIter, KvSeekIter};
use mors_traits::kms::Kms;
use mors_traits::levelctl::{Level, LEVEL0};
use mors_traits::sstable::TableTrait;

use crate::ctl::LevelCtl;
use crate::error::MorsLevelCtlError;
use crate::handler::LevelHandler;
type Result<T> = std::result::Result<T, MorsLevelCtlError>;
impl<T: TableTrait<K::Cipher>, K: Kms> LevelCtl<T, K> {
    pub(crate) fn get_impl(&self, key: &KeyTs) {
        let mut max_txn = TxnTs::default();
        // let mut max_value = None;
        for level in 0..=self.max_level().to_u8() {
            let level: Level = level.into();
            let handler = self.handler(level).unwrap();
        }
    }
}
impl<T: TableTrait<K::Cipher>, K: Kms> LevelHandler<T, K> {
    async fn get(&self, key: &KeyTs) -> Result<Option<(TxnTs, ValueMeta)>> {
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
                                        if let Some(v) = iter.value() {
                                            let txn = seek_key.txn_ts();
                                            return Some((txn, v));
                                        }
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
                        max_value = value.into();
                    }
                };
            }
            if max_txn != TxnTs::default() && max_value.is_some() {
                let value = max_value.unwrap();
                return Ok(Some((max_txn, value)));
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
        } else {
            let table_index = handler
                .tables()
                .binary_search_by(|t| t.biggest().cmp(key))
                .ok()
                .unwrap();
            if table_index >= handler.tables().len() {
                return None;
            }
            let t = handler.tables()[table_index].clone();
            if !t.may_contain(key.key()) {
                return None;
            }
            vec![t].into()
        }
    }
}
