use std::{fmt::Display, path::PathBuf};

use crate::{cache::CacheTrait, file_id::SSTableId, kms::KmsCipher, ts::KeyTs};
use mors_common::compress::CompressionType;
use std::error::Error;
use thiserror::Error;

pub trait TableTrait<C: CacheTrait<Self::Block, Self::TableIndexBuf>, K: KmsCipher>:
    Sized + Send + 'static
{
    type ErrorType: Into<SSTableError>;
    type Block: BlockTrait;
    type TableIndexBuf: TableIndexBufTrait;
    type TableBuilder: TableBuilderTrait<Self, C, K>;
    fn size(&self) -> usize;
    fn stale_data_size(&self) -> usize;
    fn id(&self) -> SSTableId;
    fn smallest(&self) -> &KeyTs;
    fn biggest(&self) -> &KeyTs;
}
pub trait TableBuilderTrait<
    T: TableTrait<C, K>,
    C: CacheTrait<T::Block, T::TableIndexBuf>,
    K: KmsCipher,
>: Default + Clone + Send + 'static
{
    fn set_compression(&mut self, compression: CompressionType);
    fn set_cache(&mut self, cache: C);
    fn set_dir(&mut self, dir: PathBuf);
    fn open(
        &self,
        id: SSTableId,
        cipher: Option<K>,
    ) -> impl std::future::Future<Output = Result<Option<T>, SSTableError>> + Send;
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
#[derive(Error, Debug)]
pub struct SSTableError(Box<dyn Error>);
impl Display for SSTableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SSTableError: {}", self.0)
    }
}
impl SSTableError {
    pub fn new<E: Error + 'static>(err: E) -> Self {
        SSTableError(Box::new(err))
    }
}
