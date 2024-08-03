use crate::default::{WithDir, WithReadOnly};
use crate::iter::{
    CacheIter, CacheIterator, DoubleEndedCacheIter, IterError, KvCacheIter,
    KvCacheIterator, KvDoubleEndedCacheIter, KvSeekIter,
};
use crate::{cache::CacheTrait, kms::KmsCipher};
use mors_common::compress::CompressionType;
use mors_common::file_id::SSTableId;
use mors_common::kv::ValueMeta;
use mors_common::ts::{KeyTs, TxnTs};
use std::borrow::Borrow;
use std::error::Error;
use std::fmt::Display;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::SystemTime;
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
    fn create_time(&self) -> SystemTime;
    fn cipher(&self) -> Option<&K>;
    fn compression(&self) -> CompressionType;
    fn iter(
        &self,
        use_cache: bool,
    ) -> impl KvCacheIterator<ValueMeta> + 'static;
}
pub trait TableBuilderTrait<T: TableTrait<K>, K: KmsCipher>:
    Default + Clone + Send + Sync + 'static + WithDir + WithReadOnly
{
    fn set_compression(&mut self, compression: CompressionType) -> &mut Self;
    fn set_cache(&mut self, cache: T::Cache) -> &mut Self;
    fn set_table_size(&mut self, size: usize) -> &mut Self;
    fn table_size(&self) -> usize;
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
pub struct CacheTableConcatIter<T: TableTrait<K>, K: KmsCipher> {
    index: Option<usize>,
    back_index: Option<usize>,
    iters: Vec<Option<Box<dyn KvCacheIterator<ValueMeta>>>>,
    tables: Vec<T>,
    k: PhantomData<K>,
    use_cache: bool,
}
impl<T: TableTrait<K>, K: KmsCipher> CacheTableConcatIter<T, K> {
    pub fn new(tables: Vec<T>, use_cache: bool) -> Self {
        let iters = Vec::with_capacity(tables.len());
        Self {
            index: None,
            back_index: None,
            iters,
            tables,
            k: PhantomData,
            use_cache,
        }
    }
    fn double_ended_eq(&self) -> bool {
        self.key() == self.key_back() && self.value() == self.value_back()
    }
}
impl<T: TableTrait<K>, K: KmsCipher> CacheIter for CacheTableConcatIter<T, K> {
    type Item = Box<dyn KvCacheIterator<ValueMeta>>;

    fn item(&self) -> Option<&Self::Item> {
        self.index
            .and_then(|index| self.iters.get(index))
            .and_then(|s| s.as_ref())
    }
}
impl<T: TableTrait<K>, K: KmsCipher> DoubleEndedCacheIter
    for CacheTableConcatIter<T, K>
{
    fn item_back(&self) -> Option<&<Self as CacheIter>::Item> {
        self.back_index
            .and_then(|index| self.iters.get(index))
            .and_then(|s| s.as_ref())
    }
}
impl<T: TableTrait<K>, K: KmsCipher> CacheIterator
    for CacheTableConcatIter<T, K>
{
    fn next(&mut self) -> Result<bool, IterError> {
        if self.double_ended_eq() {
            return Ok(false);
        }
        let new_index = match self.index {
            Some(index) => {
                if let Some(cur) = self.iters[index].as_mut() {
                    if cur.next()? {
                        return Ok(!self.double_ended_eq());
                    };
                    if index == self.tables.len() - 1 {
                        return Ok(false);
                    }
                    index + 1
                } else {
                    index
                }
            }
            None => {
                if self.tables.is_empty() {
                    return Ok(false);
                }
                0
            }
        };
        let mut iter = self.tables[new_index].iter(self.use_cache);
        if !iter.next()? {
            return Ok(false);
        }
        self.iters[new_index] = Some(Box::new(iter));
        Ok(!self.double_ended_eq())
    }
}
impl<T: TableTrait<K>, K: KmsCipher> KvSeekIter for CacheTableConcatIter<T, K> {
    fn seek(
        &mut self,
        k: mors_common::ts::KeyTsBorrow<'_>,
    ) -> Result<bool, IterError> {
        let index = self
            .tables
            .binary_search_by(|t| t.biggest().partial_cmp(&k).unwrap())
            .unwrap_or_else(|i| i);

        if index == self.tables.len() {
            return Ok(false);
        }
        if let Some(current) = self.iters[index].as_mut() {
            current.seek(k)
        } else {
            let mut iter = self.tables[index].iter(self.use_cache);
            if iter.seek(k)? {
                self.index = Some(index);
                self.iters[index] = Some(Box::new(iter));
                Ok(true)
            }else{
                Ok(false)
            }

        }
    }
}

impl<T: TableTrait<K>, K: KmsCipher> KvCacheIter<ValueMeta>
    for CacheTableConcatIter<T, K>
{
    fn key(&self) -> Option<mors_common::ts::KeyTsBorrow<'_>> {
        self.item().and_then(|x| x.key())
    }

    fn value(&self) -> Option<ValueMeta> {
        self.item().and_then(|x| x.value())
    }
}
impl<T: TableTrait<K>, K: KmsCipher> KvDoubleEndedCacheIter<ValueMeta>
    for CacheTableConcatIter<T, K>
{
    fn key_back(&self) -> Option<mors_common::ts::KeyTsBorrow<'_>> {
        self.item_back().and_then(|x| x.key())
    }

    fn value_back(&self) -> Option<ValueMeta> {
        self.item_back().and_then(|x| x.value())
    }
}
impl<T: TableTrait<K>, K: KmsCipher> KvCacheIterator<ValueMeta>
    for CacheTableConcatIter<T, K>
{
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
