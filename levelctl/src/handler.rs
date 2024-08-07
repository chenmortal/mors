use mors_common::{file_id::SSTableId, ts::TxnTs};
use mors_traits::{
    kms::Kms,
    levelctl::{Level, LEVEL0},
    sstable::TableTrait,
};
use parking_lot::RwLock;
use std::{collections::HashSet, ops::Deref};
use std::{marker::PhantomData, sync::Arc};

use crate::error::LevelHandlerError;
type Result<T> = std::result::Result<T, LevelHandlerError>;
#[derive(Clone, Debug)]
pub(crate) struct LevelHandler<T: TableTrait<K::Cipher>, K: Kms>(
    Arc<LevelHandlerInner<T, K>>,
);
impl<T: TableTrait<K::Cipher>, K: Kms> Default for LevelHandler<T, K> {
    fn default() -> Self {
        Self(Default::default())
    }
}
impl<T, K> Deref for LevelHandler<T, K>
where
    T: TableTrait<K::Cipher>,
    K: Kms,
{
    type Target = RwLock<LevelHandlerTables<T, K>>;
    fn deref(&self) -> &Self::Target {
        &self.0.table_handler
    }
}
#[derive(Debug)]
struct LevelHandlerInner<T: TableTrait<K::Cipher>, K: Kms> {
    table_handler: RwLock<LevelHandlerTables<T, K>>,
    level: Level,
}
impl<T: TableTrait<K::Cipher>, K: Kms> Default for LevelHandlerInner<T, K> {
    fn default() -> Self {
        Self {
            table_handler: Default::default(),
            level: Default::default(),
        }
    }
}
#[derive(Debug)]
pub(crate) struct LevelHandlerTables<T: TableTrait<K::Cipher>, K: Kms> {
    tables: Vec<T>,
    total_size: usize,
    total_stale_size: usize,
    k: PhantomData<K>,
}
impl<T: TableTrait<K::Cipher>, K: Kms> Default for LevelHandlerTables<T, K> {
    fn default() -> Self {
        Self {
            tables: Default::default(),
            total_size: Default::default(),
            total_stale_size: Default::default(),
            k: Default::default(),
        }
    }
}
impl<T: TableTrait<K::Cipher>, K: Kms> LevelHandler<T, K> {
    pub(crate) fn new(level: Level, tables: Vec<T>) -> Self {
        let mut table_handler: LevelHandlerTables<T, K> =
            LevelHandlerTables::default();
        table_handler.init(level, tables);

        Self(Arc::new(LevelHandlerInner {
            table_handler: RwLock::new(table_handler),
            level,
        }))
    }

    pub(crate) fn level(&self) -> Level {
        self.0.level
    }
    pub(crate) fn tables_len(&self) -> usize {
        self.read().tables.len()
    }
    pub(crate) fn total_size(&self) -> usize {
        self.read().total_size
    }

    pub(crate) fn replace(&self, old: &[T], new: &[T]) {
        let mut inner_w = self.write();
        let to_delete =
            old.iter().map(|x| x.id()).collect::<HashSet<SSTableId>>();

        let mut new_tables =
            Vec::with_capacity(inner_w.tables.len() - to_delete.len());

        inner_w
            .tables
            .drain(..)
            .filter(|t| !to_delete.contains(&t.id()))
            .for_each(|t| new_tables.push(t));
        new.iter().for_each(|t| new_tables.push(t.clone()));

        inner_w.init(self.level(), new_tables);
    }
    pub(crate) fn delete(&self, delete: &[T]) {
        let mut inner_w = self.write();
        let to_delete = delete
            .iter()
            .map(|t| t.id())
            .collect::<HashSet<SSTableId>>();
        let mut new_tables =
            Vec::with_capacity(inner_w.tables.len() - to_delete.len());
        let mut sub_total_size = 0;
        let mut sub_total_stale_size = 0;
        for table in inner_w.tables.drain(..) {
            if to_delete.contains(&table.id()) {
                sub_total_size -= table.size();
                sub_total_stale_size -= table.stale_data_size();
            } else {
                new_tables.push(table);
            }
        }
        inner_w.tables = new_tables;
        inner_w.total_size -= sub_total_size;
        inner_w.total_stale_size -= sub_total_stale_size;
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
impl<T: TableTrait<K::Cipher>, K: Kms> LevelHandlerTables<T, K> {
    pub(crate) fn init(&mut self, level: Level, tables: Vec<T>) {
        self.tables = tables;
        let mut total_size = 0;
        let mut total_stale_size = 0;
        self.tables.iter().for_each(|t| {
            total_size += t.size();
            total_stale_size += t.stale_data_size();
        });
        if level == LEVEL0 {
            self.tables.sort_by_key(|a| a.id());
        } else {
            self.tables.sort_by(|a, b| a.smallest().cmp(b.smallest()));
        }
    }
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
