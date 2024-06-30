#[cfg(not(feature = "sync"))]
use moka::future::Cache as MokaCache;
#[cfg(not(feature = "sync"))]
use moka::future::CacheBuilder as MokaCacheBuilder;
#[cfg(feature = "sync")]
use moka::sync::Cache as MokaCache;
#[cfg(feature = "sync")]
use moka::sync::CacheBuilder as MokaCacheBuilder;

use mors_traits::{
    cache::{BlockCacheKey, Cache, CacheBuilder},
    file_id::SSTableId,
    sstable::{BlockTrait, TableIndexBufTrait},
};

use crate::error::MorsCacheError;
type Result<T> = std::result::Result<T, MorsCacheError>;
pub struct MorsCache<B: BlockTrait, T: TableIndexBufTrait> {
    block_cache: Option<MokaCache<BlockCacheKey, B>>,
    index_cache: MokaCache<SSTableId, T>,
}
impl<B: BlockTrait, T: TableIndexBufTrait> Cache<B, T> for MorsCache<B, T> {
    type ErrorType = MorsCacheError;
    type CacheBuilder = MorsCacheBuilder;
    async fn get_block(&self, key: &BlockCacheKey) -> Option<B> {
        self.block_cache.as_ref()?.get(key).await
    }
    async fn insert_block(&self, key: BlockCacheKey, block: B) {
        if let Some(block_cache) = self.block_cache.as_ref() {
            block_cache.insert(key, block).await;
        }
    }
    async fn get_index(&self, key: SSTableId) -> Option<T> {
        self.index_cache.get(&key).await
    }
    async fn insert_index(&self, key: SSTableId, index: T) {
        self.index_cache.insert(key, index).await;
    }
}
#[derive(Debug)]
pub struct MorsCacheBuilder {
    block_cache_size: usize,
    block_size: usize,
    index_cache_size: usize,
    index_size: usize,
}

const DEFAULT_INDEX_SIZE: usize = ((64 << 20) as f64 * 0.05) as usize;
impl Default for MorsCacheBuilder {
    fn default() -> Self {
        Self {
            index_cache_size: 16 << 20,
            index_size: DEFAULT_INDEX_SIZE,
            block_size: 4 * 1024,
            block_cache_size: 256 << 20,
        }
    }
}
impl<B: BlockTrait, T: TableIndexBufTrait> CacheBuilder<MorsCache<B, T>, B, T>
    for MorsCacheBuilder
{
    fn build(&self) -> Result<MorsCache<B, T>> {
        let num_in_cache = (self.index_cache_size / self.index_size).max(1);
        let index_cache = MokaCacheBuilder::new(num_in_cache as u64)
            .initial_capacity(num_in_cache / 2)
            .build();
        if self.block_cache_size > 0 {
            let num_in_cache = (self.block_cache_size / self.block_size).max(1);
            let block_cache = MokaCacheBuilder::new(num_in_cache as u64)
                .initial_capacity(num_in_cache / 2)
                .build();
            Ok(MorsCache {
                block_cache: Some(block_cache),
                index_cache,
            })
        } else {
            Ok(MorsCache {
                block_cache: None,
                index_cache,
            })
        }
    }
}
impl MorsCacheBuilder {
    pub fn set_index_cache_size(&mut self, index_cache_size: usize) {
        self.index_cache_size = index_cache_size;
    }

    pub fn set_index_size(&mut self, index_size: usize) {
        self.index_size = index_size;
    }
    pub fn set_block_cache_size(&mut self, block_cache_size: usize) {
        self.block_cache_size = block_cache_size;
    }
    pub fn set_block_size(&mut self, block_size: usize) {
        self.block_size = block_size;
    }
}

impl<B: BlockTrait, T: TableIndexBufTrait> MorsCache<B, T> {
    pub async fn get_block(&self, key: &BlockCacheKey) -> Option<B> {
        self.block_cache.as_ref()?.get(key).await
    }
    pub async fn insert_block(&self, key: BlockCacheKey, block: B) {
        if let Some(block_cache) = self.block_cache.as_ref() {
            block_cache.insert(key, block).await;
        }
    }
    pub async fn get_index(&self, key: SSTableId) -> Option<T> {
        self.index_cache.get(&key).await
    }
    pub async fn insert_index(&self, key: SSTableId, index: T) {
        self.index_cache.insert(key, index).await;
    }
}
