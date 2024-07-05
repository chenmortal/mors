use error::MorsCacheError;
#[cfg(not(feature = "sync"))]
use moka::future::Cache as MokaCache;
#[cfg(not(feature = "sync"))]
use moka::future::CacheBuilder as MokaCacheBuilder;
#[cfg(feature = "sync")]
use moka::sync::Cache as MokaCache;
#[cfg(feature = "sync")]
use moka::sync::CacheBuilder as MokaCacheBuilder;

use mors_traits::{
    cache::{BlockCacheKey, CacheBuilder, CacheTrait},
    file_id::SSTableId,
};

use crate::block::Block;
use crate::table_index::TableIndexBuf;
type Result<T> = std::result::Result<T, MorsCacheError>;
mod error;

#[derive(Clone)]
pub struct Cache {
    block_cache: Option<MokaCache<BlockCacheKey, Block>>,
    index_cache: MokaCache<SSTableId, TableIndexBuf>,
}
impl Cache {
    pub(crate) async fn get_block(&self, key: &BlockCacheKey) -> Option<Block> {
        self.block_cache.as_ref()?.get(key).await
    }
    pub(crate) async fn insert_block(&self, key: BlockCacheKey, block: Block) {
        if let Some(block_cache) = self.block_cache.as_ref() {
            block_cache.insert(key, block).await;
        }
    }
    pub(crate)  async fn get_index(&self, key: SSTableId) -> Option<TableIndexBuf> {
        self.index_cache.get(&key).await
    }
    pub(crate)  async fn insert_index(&self, key: SSTableId, index: TableIndexBuf) {
        self.index_cache.insert(key, index).await;
    }
}
impl CacheTrait for Cache {
    type ErrorType = MorsCacheError;
    type CacheBuilder = MorsCacheBuilder;
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
impl CacheBuilder<Cache> for MorsCacheBuilder {
    fn build(&self) -> Result<Cache> {
        let num_in_cache = (self.index_cache_size / self.index_size).max(1);
        let index_cache = MokaCacheBuilder::new(num_in_cache as u64)
            .initial_capacity(num_in_cache / 2)
            .build();
        if self.block_cache_size > 0 {
            let num_in_cache = (self.block_cache_size / self.block_size).max(1);
            let block_cache = MokaCacheBuilder::new(num_in_cache as u64)
                .initial_capacity(num_in_cache / 2)
                .build();
            Ok(Cache {
                block_cache: Some(block_cache),
                index_cache,
            })
        } else {
            Ok(Cache {
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

// impl Cache<B, T> {
//     pub async fn get_block(&self, key: &BlockCacheKey) -> Option<B> {
//         self.block_cache.as_ref()?.get(key).await
//     }
//     pub async fn insert_block(&self, key: BlockCacheKey, block: B) {
//         if let Some(block_cache) = self.block_cache.as_ref() {
//             block_cache.insert(key, block).await;
//         }
//     }
//     pub async fn get_index(&self, key: SSTableId) -> Option<T> {
//         self.index_cache.get(&key).await
//     }
//     pub async fn insert_index(&self, key: SSTableId, index: T) {
//         self.index_cache.insert(key, index).await;
//     }
// }
