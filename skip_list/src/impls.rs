use std::mem::size_of;
use std::sync::Arc;

use mors_traits::{
    iter::KvCacheIterator,
    kv::ValueMeta,
    skip_list::{SkipListError, SkipListTrait},
};

use crate::{
    error::MorsSkipListError,
    iter::SkipListIter,
    skip_list::{Node, SkipList, SkipListInner},
};

type Result<T> = std::result::Result<T, SkipListError>;

impl SkipListTrait for SkipList {
    type ErrorType = MorsSkipListError;

    fn new(
        max_size: usize,
        cmp: fn(&[u8], &[u8]) -> std::cmp::Ordering,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            inner: Arc::new(SkipListInner::new(max_size, cmp)?),
        })
    }

    fn size(&self) -> usize {
        self.inner.arena().len()
    }

    fn push(&self, key: &[u8], value: &[u8]) -> Result<()> {
        Ok(self.inner.push(key, value)?)
    }

    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        Ok(self.inner.get(key)?)
    }
    fn get_or_next(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        Ok(self.inner.get_or_next(key)?)
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn height(&self) -> usize {
        self.inner.height()
    }

    const MAX_NODE_SIZE: usize = size_of::<Node>();

    fn iter(
        &self,
    ) -> impl KvCacheIterator<ValueMeta> {
        SkipListIter::new(&self.inner)
    }
}
impl From<MorsSkipListError> for SkipListError {
    fn from(val: MorsSkipListError) -> Self {
        SkipListError::new(val)
    }
}
