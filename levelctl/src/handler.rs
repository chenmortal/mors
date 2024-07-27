use mors_common::ts::TxnTs;
use mors_traits::{
    kms::KmsCipher,
    levelctl::{Level, LEVEL0},
    sstable::TableTrait,
};
use parking_lot::RwLock;
use std::ops::Deref;
use std::{marker::PhantomData, sync::Arc};

use crate::error::LevelHandlerError;
type Result<T> = std::result::Result<T, LevelHandlerError>;
#[derive(Clone)]
pub(crate) struct LevelHandler<T: TableTrait<K>, K: KmsCipher>(
    Arc<LevelHandlerInner<T, K>>,
);
impl<T: TableTrait<K>, K: KmsCipher> Deref for LevelHandler<T, K>
where
    T: TableTrait<K>,
    K: KmsCipher,
{
    type Target = RwLock<LevelHandlerTables<T, K>>;
    fn deref(&self) -> &Self::Target {
        &self.0.table_handler
    }
}
struct LevelHandlerInner<T: TableTrait<K>, K: KmsCipher> {
    table_handler: RwLock<LevelHandlerTables<T, K>>,
    level: Level,
}
pub(crate) struct LevelHandlerTables<T: TableTrait<K>, K: KmsCipher> {
    tables: Vec<T>,
    total_size: usize,
    total_stale_size: usize,
    k: PhantomData<K>,
}
impl<T: TableTrait<K>, K: KmsCipher> LevelHandler<T, K> {
    pub(crate) fn new(level: Level, mut tables: Vec<T>) -> Self {
        let mut total_size = 0;
        let mut total_stale_size = 0;
        tables.iter().for_each(|t| {
            total_size += t.size();
            total_stale_size += t.stale_data_size();
        });
        if level == LEVEL0 {
            tables.sort_by_key(|a| a.id());
        } else {
            tables.sort_by(|a, b| a.smallest().cmp(b.smallest()));
        }
        Self(Arc::new(LevelHandlerInner {
            table_handler: RwLock::new(LevelHandlerTables {
                tables,
                total_size,
                total_stale_size,
                k: PhantomData,
            }),
            level,
        }))
    }
    pub(crate) fn level(&self) -> &Level {
        &self.0.level
    }
    pub(crate) fn tables_len(&self) -> usize {
        self.read().tables.len()
    }
    pub(crate) fn total_size(&self) -> usize {
        self.read().total_size
    }
    pub(crate) fn validate(&self) -> Result<()> {
        let inner = self.0.table_handler.read();
        if self.0.level == LEVEL0 {
            return Ok(());
        }

        while let Some(w) = inner.tables.windows(2).next() {
            if w[0].biggest() > w[1].smallest() {
                return Err(LevelHandlerError::TableOverlapError(
                    self.0.level,
                    w[0].id(),
                    w[0].biggest().to_owned(),
                    w[1].id(),
                    w[1].smallest().to_owned(),
                ));
            }
            if w[1].smallest() >= w[1].biggest() {
                return Err(LevelHandlerError::TableInnerSortError(
                    self.0.level,
                    w[1].id(),
                    w[1].smallest().to_owned(),
                    w[1].biggest().to_owned(),
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn max_version(&self) -> TxnTs {
        let inner = self.0.table_handler.read();
        let mut max_version = TxnTs::default();
        inner.tables.iter().for_each(|t| {
            max_version = max_version.max(t.max_version());
        });
        max_version
    }
}
impl<T: TableTrait<K>, K: KmsCipher> LevelHandlerTables<T, K> {
    pub(crate) fn tables(&self) -> &[T] {
        &self.tables
    }

    pub(crate) fn total_size(&self) -> usize {
        self.total_size
    }
    pub(crate) fn total_stale_size(&self) -> usize {
        self.total_stale_size
    }
    pub(crate) fn push(&mut self, table: T) {
        self.total_size += table.size();
        self.total_stale_size += table.stale_data_size();
        self.tables.push(table);
    }
    pub(crate) fn pop(&mut self) -> Option<T> {
        self.tables.pop().map(|t| {
            self.total_size -= t.size();
            self.total_stale_size -= t.stale_data_size();
            t
        })
    }
}
