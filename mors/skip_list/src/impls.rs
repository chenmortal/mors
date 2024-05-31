use std::sync::Arc;

use mors_traits::skip_list::SkipList as SkipListTrait;

use crate::{error::MorsSkipListError, SkipList};
type Result<T> = std::result::Result<T, MorsSkipListError>;
pub struct MorsSkipList {
    inner: Arc<SkipList>,
}
impl SkipListTrait for MorsSkipList {
    type ErrorType = MorsSkipListError;

    fn new(max_size: usize, cmp: fn(&[u8], &[u8]) -> std::cmp::Ordering) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            inner: Arc::new(SkipList::new(max_size, cmp)?),
        })
    }

    fn size(&self) -> usize {
        self.inner.arena.len()
    }

    fn push(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.push(key, value)
    }

    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        self.inner.get(key)
    }
    fn get_or_next(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        self.inner.get_or_next(key)
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn height(&self) -> usize {
        self.inner.height()
    }
}
