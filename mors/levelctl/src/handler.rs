use std::{marker::PhantomData, sync::Arc};

use mors_traits::{
    cache::CacheTrait,
    kms::KmsCipher,
    levelctl::{Level, LEVEL0},
    sstable::TableTrait,
};
use parking_lot::RwLock;

use crate::error::LevelHandlerError;
type Result<T> = std::result::Result<T, LevelHandlerError>;
pub(crate) struct LevelHandler<
    T: TableTrait<C, K>,
    C: CacheTrait<T::Block, T::TableIndexBuf>,
    K: KmsCipher,
>(Arc<LevelHandlerInner<T, C, K>>);
struct LevelHandlerInner<
    T: TableTrait<C, K>,
    C: CacheTrait<T::Block, T::TableIndexBuf>,
    K: KmsCipher,
> {
    table_handler: RwLock<LevelHandlerTables<T, C, K>>,
    level: Level,
}
struct LevelHandlerTables<
    T: TableTrait<C, K>,
    C: CacheTrait<T::Block, T::TableIndexBuf>,
    K: KmsCipher,
> {
    tables: Vec<T>,
    total_size: usize,
    total_stale_size: usize,
    c: PhantomData<C>,
    k: PhantomData<K>,
}
impl<
        T: TableTrait<C, K>,
        C: CacheTrait<T::Block, T::TableIndexBuf>,
        K: KmsCipher,
    > LevelHandler<T, C, K>
{
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
                c: PhantomData,
                k: PhantomData,
            }),
            level,
        }))
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
            if w[1].smallest() >= w[1].biggest(){
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
}
