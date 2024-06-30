use bytes::BufMut;

use crate::{
    file_id::SSTableId,
    sstable::{BlockIndex, BlockTrait, TableIndexBufTrait},
};

pub trait Cache<B: BlockTrait, T: TableIndexBufTrait>:
    Sized + Send + Sync + Clone
{
    type ErrorType;
    type CacheBuilder: CacheBuilder<Self, B, T>;
    fn get_block(
        &self,
        key: &BlockCacheKey,
    ) -> impl std::future::Future<Output = Option<B>> + Send;

    fn insert_block(
        &self,
        key: BlockCacheKey,
        block: B,
    ) -> impl std::future::Future<Output = ()> + Send;

    fn get_index(
        &self,
        key: SSTableId,
    ) -> impl std::future::Future<Output = Option<T>> + Send;

    fn insert_index(
        &self,
        key: SSTableId,
        index: T,
    ) -> impl std::future::Future<Output = ()> + Send;
}
pub trait CacheBuilder<C: Cache<B, T>, B: BlockTrait, T: TableIndexBufTrait>:
    Default
{
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
