use std::cmp::Ordering;

//需满足并发安全
pub trait SkipList {
    type ErrorType;
    fn new(max_size: usize, cmp: fn(&[u8], &[u8]) -> Ordering) -> Result<Self, Self::ErrorType>
    where
        Self: Sized;
    fn size(&self) -> usize;
    fn push(&self, key: &[u8], value: &[u8]) -> Result<(), Self::ErrorType>;
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>,Self::ErrorType>;
    fn get_or_next(&self, key: &[u8]) -> Result<Option<&[u8]>,Self::ErrorType>;
    fn is_empty(&self) -> bool;
    fn height(&self) -> usize;
}

