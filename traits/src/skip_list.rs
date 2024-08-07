use crate::iter::KvCacheIterator;
use std::cmp::Ordering;
use std::error::Error;
use std::fmt::Display;
use mors_common::kv::ValueMeta;
use thiserror::Error;

//需满足并发安全
pub type OptionKV<'a> =Option<(&'a[u8], Option<&'a[u8]>)>;
pub trait SkipListTrait: Send + Sync + Clone + 'static {
    type ErrorType: Into<SkipListError>;
    fn new(
        max_size: usize,
        cmp: fn(&[u8], &[u8]) -> Ordering,
    ) -> Result<Self, SkipListError>
    where
        Self: Sized;
    fn size(&self) -> usize;
    fn push(&self, key: &[u8], value: &[u8]) -> Result<(), SkipListError>;
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, SkipListError>;
    fn get_or_next(&self, key: &[u8]) -> Result<Option<&[u8]>, SkipListError>;
    fn get_key_value(&self, key: &[u8],allow_next:bool) -> Result<OptionKV, SkipListError>;
    fn is_empty(&self) -> bool;
    fn height(&self) -> usize;
    fn iter(&self) -> impl KvCacheIterator<ValueMeta>;
    const MAX_NODE_SIZE: usize;
}

#[derive(Error, Debug)]
pub struct SkipListError(Box<dyn Error>);
impl SkipListError {
    pub fn new<E: Error + 'static>(error: E) -> Self {
        SkipListError(Box::new(error))
    }
}
impl Display for SkipListError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SkipList Error: {}", self.0)
    }
}
