#[cfg(not(feature = "sync"))]
use moka::future::Cache as MokaCache;
#[cfg(feature = "sync")]
use moka::sync::Cache as MokaCache;

use mors_traits::{
    cache::{BlockCacheKey, Cache, CacheBuilder},
    file_id::SSTableId,
    sstable::{Block, TableIndexBuf},
};

use crate::error::MorsCacheError;
type Result<T> = std::result::Result<T, MorsCacheError>;
pub struct MorsCache<B: Block, T: TableIndexBuf> {
    block_cache: Option<MokaCache<BlockCacheKey, B>>,
    index_cache: MokaCache<SSTableId, T>,
}
impl<B: Block, T: TableIndexBuf> Cache for MorsCache<B, T> {
    type ErrorType = MorsCacheError;
    type CacheBuilder = MorsCacheBuilder;
}
#[derive(Debug)]
pub struct MorsCacheBuilder {
    block_cache_size: usize,
    index_cache_size: usize,
    index_size: usize,
}

const DEFAULT_INDEX_SIZE: usize = ((64 << 20) as f64 * 0.05) as usize;
impl Default for MorsCacheBuilder {
    fn default() -> Self {
        Self {
            block_cache_size: 1024,
            index_cache_size: 16 << 20,
            index_size: DEFAULT_INDEX_SIZE,
        }
    }
}
impl<B: Block, T: TableIndexBuf> CacheBuilder<MorsCache<B, T>>
    for MorsCacheBuilder
{
    fn build(&self) -> Result<MorsCache<B, T>> {
        todo!()
    }
}
impl MorsCacheBuilder {
    pub fn set_index_cache_size(&mut self, index_cache_size: usize) {
        self.index_cache_size = index_cache_size;
    }

    pub fn set_index_size(&mut self, index_size: usize) {
        self.index_size = index_size;
    }
}
