use std::cmp::Ordering;

use flatbuffers::{Follow, Vector};
use mors_common::{kv::ValueMeta, ts::KeyTsBorrow};
use mors_traits::{
    iter::{
        CacheIter, CacheIterator, DoubleEndedCacheIter, IterError, KvCacheIter,
        KvCacheIterator, KvDoubleEndedCacheIter, KvSeekIter,
    },
    kms::KmsCipher,
};

use crate::{block::read::CacheBlockIter, table::Table};

pub struct CacheTableIter<K: KmsCipher> {
    inner: Table<K>,
    use_cache: bool,
    block_iter: Option<CacheBlockIter>,
    back_block_iter: Option<CacheBlockIter>,
}
impl<K: KmsCipher> CacheTableIter<K> {
    pub fn new(inner: Table<K>, use_cache: bool) -> Self {
        Self {
            inner,
            use_cache,
            block_iter: None,
            back_block_iter: None,
        }
    }
    fn double_ended_eq(&self) -> bool {
        if let Some(iter) = self.block_iter.as_ref() {
            if let Some(back_iter) = self.back_block_iter.as_ref() {
                if iter.key() == back_iter.key_back()
                    && iter.value() == back_iter.value_back()
                {
                    return true;
                }
            }
        }
        false
    }
}
impl<K: KmsCipher> CacheIter for CacheTableIter<K> {
    type Item = CacheBlockIter;

    fn item(&self) -> Option<&Self::Item> {
        self.block_iter.as_ref()
    }
}
impl<K: KmsCipher> DoubleEndedCacheIter for CacheTableIter<K> {
    fn item_back(&self) -> Option<&<Self as CacheIter>::Item> {
        self.back_block_iter.as_ref()
    }
}
impl<K: KmsCipher> KvCacheIter<ValueMeta> for CacheTableIter<K> {
    fn key(&self) -> Option<KeyTsBorrow<'_>> {
        self.block_iter.as_ref().and_then(|b| b.key())
    }

    fn value(&self) -> Option<ValueMeta> {
        self.block_iter.as_ref().and_then(|b| b.value())
    }
}
impl<K: KmsCipher> KvDoubleEndedCacheIter<ValueMeta> for CacheTableIter<K> {
    fn key_back(&self) -> Option<KeyTsBorrow<'_>> {
        self.back_block_iter.as_ref().and_then(|b| b.key_back())
    }

    fn value_back(&self) -> Option<ValueMeta> {
        self.back_block_iter.as_ref().and_then(|b| b.value_back())
    }
}
impl<K: KmsCipher> CacheIterator for CacheTableIter<K> {
    fn next(&mut self) -> Result<bool, IterError> {
        if self.double_ended_eq() {
            return Ok(false);
        }
        let new_block_index = match self.block_iter.as_mut() {
            Some(iter) => {
                if iter.next()? {
                    return Ok(!self.double_ended_eq());
                }
                let block_index: usize = iter.block_index().into();
                if block_index == self.inner.block_offsets_len() - 1 {
                    return Ok(false);
                }
                (block_index + 1).into()
            }
            None => {
                if self.inner.block_offsets_len() == 0 {
                    return Ok(false);
                }
                0u32.into()
            }
        };

        let next_block = self
            .inner
            .get_block(new_block_index, self.use_cache)
            .map_err(IterError::new)?;
        self.block_iter = next_block.iter().into();
        if self.block_iter.as_mut().unwrap().next()? {
            Ok(!self.double_ended_eq())
        } else {
            Ok(false)
        }
    }
}
impl<K: KmsCipher> KvSeekIter for CacheTableIter<K> {
    fn seek(&mut self, k: KeyTsBorrow<'_>) -> Result<bool, IterError> {
        let indexbuf = self.inner.get_index()?;
        // let index = match indexbuf
        // .offsets()
        // .binary_search_by(|b| b.key_ts().partial_cmp(&k).unwrap())
        let index = match binary_search_by(&indexbuf.offsets(), |b| {
            let b: KeyTsBorrow = b.key_ts().unwrap().bytes().into();
            b.partial_cmp(&k).unwrap()
        }) {
            Ok(index) => index,
            Err(index) => {
                if index >= indexbuf.offsets().len() {
                    return Ok(false);
                }
                if index == 0 {
                    return Ok(false);
                }
                index - 1
            }
        };
        let next_block = self.inner.get_block(index.into(), self.use_cache)?;
        self.block_iter = next_block.iter().into();
        self.block_iter.as_mut().unwrap().seek(k)
    }
}
impl<K: KmsCipher> KvCacheIterator<ValueMeta> for CacheTableIter<K> {}

pub fn binary_search_by<'a, T: Follow<'a> + 'a, F>(
    v: &Vector<'a, T>,
    mut f: F,
) -> Result<usize, usize>
where
    F: FnMut(T::Inner) -> Ordering,
{
    // INVARIANTS:
    // - 0 <= left <= left + size = right <= self.len()
    // - f returns Less for everything in self[..left]
    // - f returns Greater for everything in self[right..]
    let mut size = v.len();
    let mut left = 0;
    let mut right = size;
    while left < right {
        let mid = left + size / 2;

        // SAFETY: the while condition means `size` is strictly positive, so
        // `size/2 < size`. Thus `left + size/2 < left + size`, which
        // coupled with the `left + size <= self.len()` invariant means
        // we have `left + size/2 < self.len()`, and this is in-bounds.
        let cmp = f(v.get(mid));
        // let cmp = f(unsafe { self.get_unchecked(mid) });

        // This control flow produces conditional moves, which results in
        // fewer branches and instructions than if/else or matching on
        // cmp::Ordering.
        // This is x86 asm for u8: https://rust.godbolt.org/z/698eYffTx.
        left = if cmp == Ordering::Less { mid + 1 } else { left };
        right = if cmp == Ordering::Greater { mid } else { right };
        if cmp == Ordering::Equal {
            // SAFETY: same as the `get_unchecked` above
            return Ok(mid);
        }

        size = right - left;
    }

    // SAFETY: directly true from the overall invariant.
    // Note that this is `<=`, unlike the assume in the `Ok` path.
    Err(left)
}
