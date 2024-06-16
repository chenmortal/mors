use std::cmp::Ordering;
use std::error::Error;
use std::fmt::Display;

use thiserror::Error;

//需满足并发安全
pub trait SkipList: Send + Sync {
    type ErrorType: Into<SkipListError>;
    fn new(
        max_size: usize,
        cmp: fn(&[u8], &[u8]) -> Ordering,
    ) -> Result<Self, Self::ErrorType>
    where
        Self: Sized;
    fn size(&self) -> usize;
    fn push(&self, key: &[u8], value: &[u8]) -> Result<(), Self::ErrorType>;
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, Self::ErrorType>;
    fn get_or_next(&self, key: &[u8])
        -> Result<Option<&[u8]>, Self::ErrorType>;
    fn is_empty(&self) -> bool;
    fn height(&self) -> usize;
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
