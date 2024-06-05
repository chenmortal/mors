use crate::kv::ValueMeta;
use crate::ts::KeyTsBorrow;

// use crate::kv::{KeyTsBorrow, ValueMeta};

// here use async fn look at https://blog.rust-lang.org/inside-rust/2022/11/17/async-fn-in-trait-nightly.html
pub struct SinkIterRev<T> {
    iter: T,
}
impl<T> SinkIter for SinkIterRev<T>
where
    T: DoubleEndedSinkIter,
{
    type Item = <T as SinkIter>::Item;

    fn item(&self) -> Option<&Self::Item> {
        self.iter.item_back()
    }
}
impl<T> DoubleEndedSinkIter for SinkIterRev<T>
where
    T: SinkIter + DoubleEndedSinkIter,
{
    fn item_back(&self) -> Option<&<Self as SinkIter>::Item> {
        self.iter.item()
    }
}
impl<T> AsyncSinkIterator for SinkIterRev<T>
where
    T: AsyncDoubleEndedSinkIterator,
{
    type ErrorType = T::ErrorType;
    async fn next(&mut self) -> Result<(), Self::ErrorType> {
        self.iter.next_back().await
    }
}
impl<T> SinkIterator for SinkIterRev<T>
where
    T: DoubleEndedSinkIterator,
{
    type ErrorType = T::ErrorType;
    fn next(&mut self) -> Result<bool, Self::ErrorType> {
        self.iter.next_back()
    }
}
impl<T> DoubleEndedSinkIterator for SinkIterRev<T>
where
    T: DoubleEndedSinkIterator,
{
    fn next_back(&mut self) -> Result<bool, Self::ErrorType> {
        self.iter.next()
    }
}
impl<T> AsyncDoubleEndedSinkIterator for SinkIterRev<T>
where
    T: AsyncDoubleEndedSinkIterator,
{
    async fn next_back(&mut self) -> Result<(), Self::ErrorType> {
        self.iter.next().await
    }
}
impl<'a, T, V> KvSinkIter<V> for SinkIterRev<T>
where
    T: KvDoubleEndedSinkIter<V>,
    V: Into<ValueMeta>,
{
    fn key(&self) -> Option<KeyTsBorrow<'_>> {
        self.iter.key_back()
    }

    fn value(&self) -> Option<V> {
        self.iter.value_back()
    }
}
impl<T, V> KvDoubleEndedSinkIter<V> for SinkIterRev<T>
where
    T: KvSinkIter<V> + KvDoubleEndedSinkIter<V>,
    V: Into<ValueMeta>,
{
    fn key_back(&self) -> Option<KeyTsBorrow<'_>> {
        self.iter.key()
    }

    fn value_back(&self) -> Option<V> {
        self.iter.value()
    }
}
pub(crate) trait SinkIter {
    type Item;
    fn item(&self) -> Option<&Self::Item>;
}
pub(crate) trait DoubleEndedSinkIter: SinkIter {
    fn item_back(&self) -> Option<&<Self as SinkIter>::Item>;
}
pub(crate) trait AsyncSinkIterator: SinkIter {
    type ErrorType;
    async fn next(&mut self) -> Result<(), Self::ErrorType>;
    async fn rev(self) -> SinkIterRev<Self>
    where
        Self: Sized + AsyncDoubleEndedSinkIterator,
    {
        SinkIterRev { iter: self }
    }
}
pub(crate) trait SinkIterator {
    type ErrorType;
    fn next(&mut self) -> Result<bool, Self::ErrorType>;
    fn rev(self) -> SinkIterRev<Self>
    where
        Self: Sized + DoubleEndedSinkIterator,
    {
        SinkIterRev { iter: self }
    }
}
pub(crate) trait DoubleEndedSinkIterator: SinkIterator {
    fn next_back(&mut self) -> Result<bool, Self::ErrorType>;
}
pub(crate) trait AsyncDoubleEndedSinkIterator:
    AsyncSinkIterator + DoubleEndedSinkIter
{
    async fn next_back(&mut self) -> Result<(), Self::ErrorType>;
}
pub(crate) trait KvSinkIter<V>
where
    V: Into<ValueMeta>,
{
    fn key(&self) -> Option<KeyTsBorrow<'_>>;
    fn value(&self) -> Option<V>;
}
pub(crate) trait KvDoubleEndedSinkIter<V>
where
    V: Into<ValueMeta>,
{
    fn key_back(&self) -> Option<KeyTsBorrow<'_>>;
    fn value_back(&self) -> Option<V>;
}
// if true then KvSinkIter.key() >= k
pub(crate) trait KvSeekIter: SinkIterator {
    fn seek(&mut self, k: KeyTsBorrow<'_>) -> Result<bool, Self::ErrorType>;
}
mod test {
    use bytes::Buf;

    use crate::kv::ValueMeta;
    use crate::ts::KeyTsBorrow;

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

    impl SinkIter for TestIter {
        type Item = [u8; 8];

        fn item(&self) -> Option<&Self::Item> {
            self.data.as_ref()
        }
    }

    impl DoubleEndedSinkIter for TestIter {
        fn item_back(&self) -> Option<&<Self as SinkIter>::Item> {
            self.back_data.as_ref()
        }
    }

    impl SinkIterator for TestIter {
        type ErrorType = TestIterError;
        fn next(&mut self) -> Result<bool, Self::ErrorType> {
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

    impl DoubleEndedSinkIterator for TestIter {
        fn next_back(&mut self) -> Result<bool, Self::ErrorType> {
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

    impl KvSinkIter<ValueMeta> for TestIter {
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

    impl KvDoubleEndedSinkIter<ValueMeta> for TestIter {
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
        fn seek(&mut self, k: KeyTsBorrow<'_>) -> Result<bool, Self::ErrorType> {
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
