use mors_common::kv::ValueMeta;
use mors_common::ts::KeyTsBorrow;
use std::error::Error;
use std::fmt::Display;
use std::ops::{Deref, DerefMut};
use thiserror::Error;

// here use async fn look at https://blog.rust-lang.org/inside-rust/2022/11/17/async-fn-in-trait-nightly.html
type Result<T> = std::result::Result<T, IterError>;
pub struct CacheIterRev<T> {
    iter: T,
}
impl<T> CacheIter for CacheIterRev<T>
where
    T: DoubleEndedCacheIter,
{
    type Item = <T as CacheIter>::Item;

    fn item(&self) -> Option<&Self::Item> {
        self.iter.item_back()
    }
}
impl<T> DoubleEndedCacheIter for CacheIterRev<T>
where
    T: CacheIter + DoubleEndedCacheIter,
{
    fn item_back(&self) -> Option<&<Self as CacheIter>::Item> {
        self.iter.item()
    }
}
impl<T> AsyncCacheIterator for CacheIterRev<T>
where
    T: AsyncDoubleEndedCacheIterator,
{
    async fn next(&mut self) -> Result<()> {
        self.iter.next_back().await
    }
}
impl<T> CacheIterator for CacheIterRev<T>
where
    T: DoubleEndedCacheIterator,
{
    fn next(&mut self) -> Result<bool> {
        self.iter.next_back()
    }
}
impl<T> DoubleEndedCacheIterator for CacheIterRev<T>
where
    T: DoubleEndedCacheIterator,
{
    fn next_back(&mut self) -> Result<bool> {
        self.iter.next()
    }
}
impl<T> AsyncDoubleEndedCacheIterator for CacheIterRev<T>
where
    T: AsyncDoubleEndedCacheIterator,
{
    async fn next_back(&mut self) -> Result<()> {
        self.iter.next().await
    }
}
impl<T, V> KvCacheIter<V> for CacheIterRev<T>
where
    T: KvDoubleEndedCacheIter<V>,
    V: Into<ValueMeta>,
{
    fn key(&self) -> Option<KeyTsBorrow<'_>> {
        self.iter.key_back()
    }

    fn value(&self) -> Option<ValueMeta> {
        self.iter.value_back()
    }
}
impl<T, V> KvDoubleEndedCacheIter<V> for CacheIterRev<T>
where
    T: KvCacheIter<V> + KvDoubleEndedCacheIter<V>,
    V: Into<ValueMeta>,
{
    fn key_back(&self) -> Option<KeyTsBorrow<'_>> {
        self.iter.key()
    }

    fn value_back(&self) -> Option<ValueMeta> {
        self.iter.value()
    }
}
pub trait CacheIter {
    type Item;
    fn item(&self) -> Option<&Self::Item>;
}
pub trait DoubleEndedCacheIter: CacheIter {
    fn item_back(&self) -> Option<&<Self as CacheIter>::Item>;
}
pub trait AsyncCacheIterator: CacheIter {
    fn next(&mut self) -> impl std::future::Future<Output = Result<()>>;
    fn rev(self) -> impl std::future::Future<Output = CacheIterRev<Self>>
    where
        Self: Sized + AsyncDoubleEndedCacheIterator,
    {
        async { CacheIterRev { iter: self } }
    }
}
pub trait CacheIterator {
    fn next(&mut self) -> Result<bool>;
    fn rev(self) -> CacheIterRev<Self>
    where
        Self: Sized + DoubleEndedCacheIterator,
    {
        CacheIterRev { iter: self }
    }
}
pub trait DoubleEndedCacheIterator: CacheIterator {
    fn next_back(&mut self) -> Result<bool>;
}
pub trait AsyncDoubleEndedCacheIterator:
    AsyncCacheIterator + DoubleEndedCacheIter
{
    fn next_back(&mut self) -> impl std::future::Future<Output = Result<()>>;
}
pub trait KvCacheIter<V>
where
    V: Into<ValueMeta>,
{
    fn key(&self) -> Option<KeyTsBorrow<'_>>;
    fn value(&self) -> Option<ValueMeta>;
}
pub trait KvCacheIterator<V>: CacheIterator + KvCacheIter<V> + Send
where
    V: Into<ValueMeta>,
{
}
pub trait KvDoubleEndedCacheIter<V>
where
    V: Into<ValueMeta>,
{
    fn key_back(&self) -> Option<KeyTsBorrow<'_>>;
    fn value_back(&self) -> Option<ValueMeta>;
}
// if true then KvCacheIter.key() >= k
pub trait KvSeekIter: CacheIterator {
    fn seek(&mut self, k: KeyTsBorrow<'_>) -> Result<bool>;
}

pub struct KvCacheMergeNode {
    valid: bool,
    iter: Box<dyn KvCacheIterator<ValueMeta>>,
}
impl Deref for KvCacheMergeNode {
    type Target = Box<dyn KvCacheIterator<ValueMeta>>;

    fn deref(&self) -> &Self::Target {
        &self.iter
    }
}
impl DerefMut for KvCacheMergeNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.iter
    }
}
impl From<Box<dyn KvCacheIterator<ValueMeta>>> for KvCacheMergeNode {
    fn from(iter: Box<dyn KvCacheIterator<ValueMeta>>) -> Self {
        Self { valid: true, iter }
    }
}
impl From<KvCacheMergeIterator> for KvCacheMergeNode {
    fn from(value: KvCacheMergeIterator) -> Self {
        Self {
            valid: true,
            iter: Box::new(value),
        }
    }
}
pub struct KvCacheMergeIterator {
    left: KvCacheMergeNode,
    right: Option<KvCacheMergeNode>,
    temp_key: Vec<u8>,
    left_small: bool,
}
impl KvCacheMergeIterator {
    pub(crate) fn new(
        mut iters: Vec<Box<dyn KvCacheIterator<ValueMeta>>>,
    ) -> Option<Self> {
        let new = |left, right| Self {
            left,
            right,
            temp_key: Vec::new(),
            left_small: true,
        };
        match iters.len() {
            0 => None,
            1 => new(iters.pop().unwrap().into(), None).into(),
            2 => {
                let right = iters.pop().unwrap();
                let left = iters.pop().unwrap();
                new(left.into(), Some(right.into())).into()
            }
            len => {
                let mid = len / 2;
                let right = iters.drain(mid..).collect::<Vec<_>>();
                let left = iters;
                new(
                    Self::new(left).unwrap().into(),
                    Some(Self::new(right).unwrap().into()),
                )
                .into()
            }
        }
    }
    fn smaller(&self) -> &KvCacheMergeNode {
        if self.left_small {
            &self.left
        } else {
            self.right.as_ref().unwrap()
        }
    }
    fn smaller_mut(&mut self) -> &mut KvCacheMergeNode {
        if self.left_small {
            &mut self.left
        } else {
            self.right.as_mut().unwrap()
        }
    }
    fn bigger(&self) -> &KvCacheMergeNode {
        if self.left_small {
            self.right.as_ref().unwrap()
        } else {
            &self.left
        }
    }
    fn bigger_mut(&mut self) -> &mut KvCacheMergeNode {
        if self.left_small {
            self.right.as_mut().unwrap()
        } else {
            &mut self.left
        }
    }
}
impl CacheIterator for KvCacheMergeIterator {
    fn next(&mut self) -> Result<bool> {
        while self.smaller().valid {
            if let Some(k) = self.smaller().key() {
                if self.temp_key.as_slice() != k.as_ref() {
                    self.temp_key = k.to_vec();
                    return Ok(true);
                }
            }

            let result = self.smaller_mut().next()?;
            if self.bigger().valid {
                if result {
                    if self.bigger().key().is_none()
                        && !self.bigger_mut().next()?
                    {
                        continue;
                    }
                    match self.smaller().key().cmp(&self.bigger().key()) {
                        std::cmp::Ordering::Less => {}
                        std::cmp::Ordering::Equal => {
                            self.bigger_mut().next()?;
                        }
                        std::cmp::Ordering::Greater => {
                            self.left_small = !self.left_small;
                        }
                    };
                } else {
                    self.left_small = !self.left_small;
                }
            }
        }
        Ok(false)
    }
}
impl KvCacheIter<ValueMeta> for KvCacheMergeIterator {
    fn key(&self) -> Option<KeyTsBorrow<'_>> {
        self.smaller().key()
    }

    fn value(&self) -> Option<ValueMeta> {
        self.smaller().value()
    }
}
impl KvCacheIterator<ValueMeta> for KvCacheMergeIterator {}

#[derive(Error, Debug)]
pub struct IterError(Box<dyn Error>);
impl Display for IterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IterError: {}", self.0)
    }
}
impl IterError {
    pub fn new<E: Error + 'static>(err: E) -> Self {
        IterError(Box::new(err))
    }
}
mod test {
    use bytes::Buf;

    use super::*;

    #[derive(Debug)]
    pub(crate) enum TestIterError {
        TestError,
    }

    pub(crate) struct TestIter {
        data: Option<[u8; 8]>,
        len: u64,
        back_data: Option<[u8; 8]>,
    }

    impl TestIter {
        pub(crate) fn new(len: usize) -> Self {
            Self {
                data: None,
                len: len as u64,
                back_data: None,
            }
        }
    }

    impl CacheIter for TestIter {
        type Item = [u8; 8];

        fn item(&self) -> Option<&Self::Item> {
            self.data.as_ref()
        }
    }

    impl DoubleEndedCacheIter for TestIter {
        fn item_back(&self) -> Option<&<Self as CacheIter>::Item> {
            self.back_data.as_ref()
        }
    }

    impl CacheIterator for TestIter {
        fn next(&mut self) -> Result<bool> {
            if let Some(d) = self.data.as_mut() {
                let now = (*d).as_ref().get_u64();
                if now + 1 == self.len {
                    return Ok(false);
                }
                if let Some(back_data) = self.back_data.as_ref() {
                    let mut b = back_data.as_ref();
                    let b = b.get_u64();
                    if now + 1 == b {
                        return Ok(false);
                    };
                };
                *d = (now + 1).to_be_bytes();
                return Ok(true);
            }
            self.data = 0u64.to_be_bytes().into();
            Ok(true)
        }
    }

    impl DoubleEndedCacheIterator for TestIter {
        fn next_back(&mut self) -> Result<bool> {
            if let Some(d) = self.back_data.as_mut() {
                let now = (*d).as_ref().get_u64();
                if now == 0 {
                    return Ok(false);
                }
                if let Some(data) = self.data.as_ref() {
                    let mut b = data.as_ref();
                    let s = b.get_u64();
                    if now - 1 == s {
                        return Ok(false);
                    }
                }

                *d = (now - 1).to_be_bytes();
                return Ok(true);
            }
            self.back_data = (self.len - 1).to_be_bytes().into();
            Ok(true)
        }
    }

    impl KvCacheIter<ValueMeta> for TestIter {
        fn key(&self) -> Option<KeyTsBorrow<'_>> {
            if let Some(s) = self.item() {
                return Some(s.as_ref().into());
            }
            None
        }

        fn value(&self) -> Option<ValueMeta> {
            if let Some(s) = self.item().cloned() {
                let mut value = ValueMeta::default();
                value.set_value(s.to_vec().into());
                return Some(value);
            }
            None
        }
    }

    impl KvDoubleEndedCacheIter<ValueMeta> for TestIter {
        fn key_back(&self) -> Option<KeyTsBorrow<'_>> {
            if let Some(s) = self.item_back() {
                return Some(s.as_ref().into());
            }
            None
        }

        fn value_back(&self) -> Option<ValueMeta> {
            if let Some(s) = self.item_back().cloned() {
                let mut value = ValueMeta::default();
                value.set_value(s.to_vec().into());
                return Some(value);
            }
            None
        }
    }

    impl KvSeekIter for TestIter {
        fn seek(&mut self, k: KeyTsBorrow<'_>) -> Result<bool> {
            let key = k.as_ref().get_u64();
            if key >= self.len {
                return Ok(false);
            }
            self.data = Some(key.to_be_bytes());
            Ok(true)
        }
    }

    #[test]
    fn test_next() {
        let len = 3;
        let split = 2;
        let mut iter = TestIter::new(len);
        crate::test_iter_next!(iter, len);

        let iter = TestIter::new(len);
        crate::test_iter_rev_next!(iter, len);

        let iter = TestIter::new(len);
        crate::test_iter_rev_rev_next!(iter, len);

        let mut iter = TestIter::new(len);
        crate::test_iter_next_back!(iter, len);

        let iter = TestIter::new(len);
        crate::test_iter_rev_next_back!(iter, len);

        let mut iter = TestIter::new(len);
        crate::test_iter_double_ended!(iter, len, split);

        let iter = TestIter::new(len);
        crate::test_iter_rev_double_ended!(iter, len, split);
    }
}
#[macro_export]
macro_rules! test_iter_next {
    ($iter:ident, $len:expr) => {
        let mut test_iter = TestIter::new($len);
        while $iter.next().unwrap() {
            assert!(test_iter.next().unwrap());
            assert_eq!($iter.key(), test_iter.key());
            assert_eq!($iter.value(), test_iter.value());
        }
    };
}
#[macro_export]
macro_rules! test_iter_rev_next {
    ($iter:ident, $len:expr) => {
        let mut iter = $iter.rev();
        let mut test_iter = TestIter::new($len).rev();
        while iter.next().unwrap() {
            assert!(test_iter.next().unwrap());
            assert_eq!(iter.key(), test_iter.key());
            assert_eq!(iter.value(), test_iter.value());
        }
    };
}
#[macro_export]
macro_rules! test_iter_rev_rev_next {
    ($iter:expr, $len:expr) => {
        let mut iter = $iter.rev().rev();
        let mut test_iter = TestIter::new($len);
        while iter.next().unwrap() {
            assert!(test_iter.next().unwrap());
            assert_eq!(iter.key(), test_iter.key());
            assert_eq!(iter.value(), test_iter.value());
        }
    };
}
#[macro_export]
macro_rules! test_iter_next_back {
    ($iter:expr, $len:expr) => {
        let mut test_iter = TestIter::new($len);
        while $iter.next_back().unwrap() {
            assert!(test_iter.next_back().unwrap());
            assert_eq!($iter.key_back(), test_iter.key_back());
            assert_eq!($iter.value_back(), test_iter.value_back());
        }
    };
}
#[macro_export]
macro_rules! test_iter_rev_next_back {
    ($iter:expr, $len:expr) => {
        let mut iter = $iter.rev();
        let mut test_iter = TestIter::new($len).rev();
        while iter.next_back().unwrap() {
            assert!(test_iter.next_back().unwrap());
            assert_eq!(iter.key_back(), test_iter.key_back());
            assert_eq!(iter.value_back(), test_iter.value_back());
        }
    };
}
#[macro_export]
macro_rules! test_iter_double_ended {
    ($iter:expr, $len:expr,$split:expr) => {
        let mut test_iter = TestIter::new($len);
        for _ in 0..$split {
            assert!($iter.next().unwrap());
            assert!(test_iter.next().unwrap());
            assert_eq!($iter.key(), test_iter.key());
            assert_eq!($iter.value(), test_iter.value());
        }
        for _ in $split..$len {
            assert!($iter.next_back().unwrap());
            assert!(test_iter.next_back().unwrap());
            assert_eq!($iter.key_back(), test_iter.key_back());
            assert_eq!($iter.value_back(), test_iter.value_back());
        }
        assert!(!$iter.next().unwrap());
        assert!(!$iter.next_back().unwrap());
    };
}
#[macro_export]
macro_rules! test_iter_rev_double_ended {
    ($iter:expr, $len:expr,$split:expr) => {
        let mut iter = $iter.rev();
        let mut test_iter = TestIter::new($len).rev();
        for _ in 0..$split {
            assert!(iter.next().unwrap());
            assert!(test_iter.next().unwrap());
            assert_eq!(iter.key(), test_iter.key());
            assert_eq!(iter.value(), test_iter.value());
        }
        for _ in $split..$len {
            assert!(iter.next_back().unwrap());
            assert!(test_iter.next_back().unwrap());
            assert_eq!(iter.key_back(), test_iter.key_back());
            assert_eq!(iter.value_back(), test_iter.value_back());
        }
        assert!(!iter.next().unwrap());
        assert!(!iter.next_back().unwrap());
    };
}
