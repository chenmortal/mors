use std::fmt::Display;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use crate::default::{WithDir, WithReadOnly};
use crate::iter::KvCacheIterator;
use crate::kv::ValueMeta;
use crate::ts::TxnTs;
use crate::{cache::CacheTrait, file_id::SSTableId, kms::KmsCipher, ts::KeyTs};
use mors_common::compress::CompressionType;
use std::error::Error;
use thiserror::Error;

pub trait TableTrait<K: KmsCipher>:
    Sized + Send + Sync + Clone + 'static
{
    type ErrorType: Into<SSTableError>;
    type Block: BlockTrait;
    type TableIndexBuf: TableIndexBufTrait;
    type TableBuilder: TableBuilderTrait<Self, K>;
    type Cache: CacheTrait;
    // type Cache: CacheTrait<Self::Block, Self::TableIndexBuf>;
    fn size(&self) -> usize;
    fn stale_data_size(&self) -> usize;
    fn id(&self) -> SSTableId;
    fn smallest(&self) -> &KeyTs;
    fn biggest(&self) -> &KeyTs;
    fn max_version(&self) -> TxnTs;
    fn cipher(&self) -> Option<&K>;
    fn compression(&self) -> CompressionType;
}
pub trait TableBuilderTrait<T: TableTrait<K>, K: KmsCipher>:
    Default + Clone + Send + Sync + 'static + WithDir + WithReadOnly
{
    fn set_compression(&mut self, compression: CompressionType);
    fn set_cache(&mut self, cache: T::Cache);
    fn open(
        &self,
        id: SSTableId,
        cipher: Option<K>,
    ) -> impl std::future::Future<Output = Result<Option<T>, SSTableError>> + Send;
    fn build_l0<I: KvCacheIterator<V>, V: Into<ValueMeta>>(
        &self,
        iter: I,
        next_id: Arc<AtomicU32>,
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
unsafe impl Send for SSTableError {}
