use std::{marker::PhantomData, sync::Arc};

use mors_traits::{
    cache::Cache,
    kms::KmsCipher,
    levelctl::{Level, LEVEL0},
    sstable::{BlockTrait, TableIndexBufTrait, TableTrait},
};
use parking_lot::RwLock;

pub(crate) struct LevelHandler<
    T: TableTrait<C, B, TB, K>,
    C: Cache<B, TB>,
    B: BlockTrait,
    TB: TableIndexBufTrait,
    K: KmsCipher,
>(Arc<LevelHandlerInner<T, C, B, TB, K>>);
struct LevelHandlerInner<
    T: TableTrait<C, B, TB, K>,
    C: Cache<B, TB>,
    B: BlockTrait,
    TB: TableIndexBufTrait,
    K: KmsCipher,
> {
    table_handler: RwLock<LevelHandlerTables<T, C, B, TB, K>>,
    level: Level,
}
struct LevelHandlerTables<
    T: TableTrait<C, B, TB, K>,
    C: Cache<B, TB>,
    B: BlockTrait,
    TB: TableIndexBufTrait,
    K: KmsCipher,
> {
    tables: Vec<T>,
    total_size: usize,
    total_stale_size: usize,
    c: PhantomData<C>,
    b: PhantomData<B>,
    tb: PhantomData<TB>,
    k: PhantomData<K>,
}
impl<
        T: TableTrait<C, B, TB, K>,
        C: Cache<B, TB>,
        B: BlockTrait,
        TB: TableIndexBufTrait,
        K: KmsCipher,
    > LevelHandler<T, C, B, TB, K>
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
                b: PhantomData,
                tb: PhantomData,
                k: PhantomData,
            }),
            level,
        }))
    }
}
