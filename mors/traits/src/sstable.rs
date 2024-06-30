use std::path::PathBuf;

use mors_common::compress::CompressionType;

use crate::{cache::Cache, file_id::SSTableId, kms::KmsCipher};

pub trait TableTrait<C: Cache<B, T>, B: BlockTrait, T: TableIndexBufTrait>:
    Sized
{
    type ErrorType;
    type TableBuilder: TableBuilderTrait<Self, C, B, T>;
}
pub trait TableBuilderTrait<
    T: TableTrait<C, B, TB>,
    C: Cache<B, TB>,
    B: BlockTrait,
    TB: TableIndexBufTrait,
>: Default
{
    fn set_compression(&mut self, compression: CompressionType);
    fn set_cache(&mut self, cache: C);
    fn set_dir(&mut self, dir: PathBuf);
    fn open<K: KmsCipher>(
        &self,
        id: SSTableId,
        cipher: Option<K>,
    ) -> Result<T, T::ErrorType>;
}
pub trait BlockTrait: Sized + Clone + Send + Sync + 'static {}
pub trait TableIndexBufTrait: Sized + Clone + Send + Sync + 'static {}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct BlockIndex(u32);
impl From<u32> for BlockIndex {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl From<BlockIndex> for u32 {
    fn from(val: BlockIndex) -> Self {
        val.0
    }
}
impl From<usize> for BlockIndex {
    fn from(value: usize) -> Self {
        Self(value as u32)
    }
}
impl From<BlockIndex> for usize {
    fn from(val: BlockIndex) -> Self {
        val.0 as usize
    }
}
