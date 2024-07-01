use std::{marker::PhantomData, sync::Arc};

use mors_traits::{
    cache::Cache,
    kms::Kms,
    sstable::{BlockTrait, TableIndexBufTrait, TableTrait},
};
use parking_lot::RwLock;

pub(crate) struct LevelHandler<
    T: TableTrait<C, B, TB, K::Cipher>,
    C: Cache<B, TB>,
    B: BlockTrait,
    TB: TableIndexBufTrait,
    K: Kms,
>(Arc<LevelHandlerInner<T, C, B, TB, K>>);
struct LevelHandlerInner<
    T: TableTrait<C, B, TB, K::Cipher>,
    C: Cache<B, TB>,
    B: BlockTrait,
    TB: TableIndexBufTrait,
    K: Kms,
> {
    table_handler: RwLock<LevelHandlerTables<T, C, B, TB, K>>,
}
struct LevelHandlerTables<
    T: TableTrait<C, B, TB, K::Cipher>,
    C: Cache<B, TB>,
    B: BlockTrait,
    TB: TableIndexBufTrait,
    K: Kms,
> {
    tables: Vec<T>,
    total_size: usize,
    total_stable_size: usize,
    c: PhantomData<C>,
    b: PhantomData<B>,
    tb: PhantomData<TB>,
    k: PhantomData<K>,
}
