use bytes::BufMut;

use crate::{file_id::SSTableId, sstable::BlockIndex};

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
impl BlockCacheKey {
    fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(8);
        v.put_u32(self.0 .0.into());
        v.put_u32(self.0 .1.into());
        v
    }
}
