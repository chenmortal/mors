use std::sync::Arc;

use mors_traits::skip_list::SkipList;

use crate::{error::MorsSkipListError, Inner};
type Result<T> = std::result::Result<T, MorsSkipListError>;
pub struct MorsSkipList {
    inner: Arc<Inner>,
}
impl SkipList for MorsSkipList {
    type ErrorType = MorsSkipListError;

    fn new(max_size: usize, cmp: fn(&[u8], &[u8]) -> std::cmp::Ordering) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            inner: Arc::new(Inner::new(max_size, cmp)?),
        })
    }

    fn size(&self) -> usize {
        self.inner.arena.len()
    }

    fn push(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.push(key, value)
    }

    fn get(&self, key: &[u8], allow_near: bool) -> Option<&[u8]> {
        todo!()
    }

    fn get_key_value(&self, key: &[u8], allow_near: bool) -> Option<(&[u8], &[u8])> {
        todo!()
    }

    fn is_empty(&self) -> bool {
        todo!()
    }

    fn height(&self) -> usize {
        todo!()
    }
}
