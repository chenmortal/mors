use mors_common::file_id::SSTableId;

use crate::sstable::BlockIndex;

pub trait CacheTrait: Sized + Send + Sync + Clone + 'static {
    type ErrorType;
    type CacheBuilder: CacheBuilder<Self>;
}
pub trait CacheBuilder<C: CacheTrait>: Default {
    fn build(&self) -> Result<C, C::ErrorType>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockCacheKey((SSTableId, BlockIndex));
impl From<(SSTableId, BlockIndex)> for BlockCacheKey {
    fn from(value: (SSTableId, BlockIndex)) -> Self {
        Self(value)
    }
}
