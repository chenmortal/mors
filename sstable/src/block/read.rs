use bytes::{Buf, BufMut};
use mors_common::{kv::ValueMeta, ts::KeyTsBorrow};
use mors_traits::{
    iter::{
        CacheIter, CacheIterator, DoubleEndedCacheIter,
        DoubleEndedCacheIterator, IterError, KvCacheIter,
        KvDoubleEndedCacheIter, KvSeekIter,
    },
    sstable::BlockIndex,
};

use crate::block::Block;
#[derive(Default)]
pub(crate) struct BlockEntryHeader {
    overlap: u16,
    diff: u16,
}
impl BlockEntryHeader {
    pub(crate) const HEADER_SIZE: usize = 4;

    pub fn new(overlap: u16, diff: u16) -> Self {
        Self { overlap, diff }
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(Self::HEADER_SIZE);
        out.put_u16(self.overlap);
        out.put_u16(self.diff);
        out
    }
    pub fn decode(mut buf: &[u8]) -> Self {
        let overlap = buf.get_u16();
        let diff = buf.get_u16();
        Self { overlap, diff }
    }
}
#[derive(Default)]
pub struct CacheBlockIter {
    inner: Block,
    base_key: Vec<u8>,
    key: Vec<u8>,
    header: BlockEntryHeader,
    entry_index: Option<usize>,
    back_key: Vec<u8>,
    back_header: BlockEntryHeader,
    back_entry_index: Option<usize>,
}
impl From<Block> for CacheBlockIter {
    fn from(inner: Block) -> Self {
        Self {
            inner,
            ..Default::default()
        }
    }
}
impl CacheBlockIter {
    fn set_entry_index(&mut self, entry_index: usize) {
        self.entry_index = Some(entry_index);
        let entry_offset = self.inner.entry_offsets()[entry_index] as usize;
        let data = &self.inner.data()[entry_offset..];
        let next_header =
            BlockEntryHeader::decode(&data[..BlockEntryHeader::HEADER_SIZE]);
        let prev_overlap = self.header.overlap as usize;
        let next_overlap = next_header.overlap as usize;
        if next_overlap > prev_overlap {
            self.key.truncate(prev_overlap);
            self.key
                .extend_from_slice(&self.base_key[prev_overlap..next_overlap]);
        } else {
            self.key.truncate(next_overlap);
        }
        self.key.extend_from_slice(
            &data[BlockEntryHeader::HEADER_SIZE
                ..BlockEntryHeader::HEADER_SIZE + next_header.diff as usize],
        );
        self.header = next_header;
    }
    pub(crate) fn block_index(&self) -> BlockIndex {
        self.inner.block_index()
    }
}
impl CacheIter for CacheBlockIter {
    type Item = usize;

    fn item(&self) -> Option<&Self::Item> {
        self.entry_index.as_ref()
    }
}
impl DoubleEndedCacheIter for CacheBlockIter {
    fn item_back(&self) -> Option<&<Self as CacheIter>::Item> {
        self.back_entry_index.as_ref()
    }
}
impl CacheIterator for CacheBlockIter {
    fn next(&mut self) -> Result<bool, IterError> {
        match self.entry_index {
            Some(id) => {
                match self.back_entry_index {
                    Some(back_id) => {
                        if id + 1 == back_id {
                            return Ok(false);
                        }
                    }
                    None => {
                        if id == self.inner.entry_offsets().len() - 1 {
                            return Ok(false);
                        }
                    }
                }
                self.set_entry_index(id + 1);
                Ok(true)
            }
            None => {
                if self.inner.entry_offsets().is_empty() {
                    return Ok(false);
                }

                if self.base_key.is_empty() {
                    let data = self.inner.data();
                    let header = BlockEntryHeader::decode(
                        &data[..BlockEntryHeader::HEADER_SIZE],
                    );
                    self.base_key = data[BlockEntryHeader::HEADER_SIZE
                        ..BlockEntryHeader::HEADER_SIZE + header.diff as usize]
                        .to_vec();
                    self.header = header;
                }
                self.key = self.base_key.to_vec();
                self.entry_index = 0.into();
                Ok(true)
            }
        }
    }
}
impl DoubleEndedCacheIterator for CacheBlockIter {
    fn next_back(&mut self) -> Result<bool, IterError> {
        match self.back_entry_index {
            Some(back_id) => {
                match self.entry_index {
                    Some(id) => {
                        if back_id - 1 == id {
                            return Ok(false);
                        }
                    }
                    None => {
                        if back_id == 0 {
                            return Ok(false);
                        }
                    }
                }

                self.back_entry_index = Some(back_id - 1);
                let next_back_entry_offset =
                    self.inner.entry_offsets()[back_id - 1] as usize;
                let data = &self.inner.data()[next_back_entry_offset..];
                let next_back_header = BlockEntryHeader::decode(
                    &data[..BlockEntryHeader::HEADER_SIZE],
                );
                let prev_back_overlap = self.back_header.overlap as usize;
                let next_back_overlap = next_back_header.overlap as usize;

                if next_back_overlap > prev_back_overlap {
                    self.back_key.truncate(prev_back_overlap);
                    self.back_key.extend_from_slice(
                        &self.base_key[prev_back_overlap..next_back_overlap],
                    );
                } else {
                    self.back_key.truncate(next_back_overlap);
                }
                self.back_key.extend_from_slice(
                    &data[BlockEntryHeader::HEADER_SIZE
                        ..BlockEntryHeader::HEADER_SIZE
                            + next_back_header.diff as usize],
                );

                self.back_header = next_back_header;
                Ok(true)
            }
            None => {
                if self.inner.entry_offsets().is_empty() {
                    return Ok(false);
                }

                if self.base_key.is_empty() {
                    let data = self.inner.data();
                    let header = BlockEntryHeader::decode(
                        &data[..BlockEntryHeader::HEADER_SIZE],
                    );
                    self.base_key = data[BlockEntryHeader::HEADER_SIZE
                        ..BlockEntryHeader::HEADER_SIZE + header.diff as usize]
                        .to_vec();
                    self.header = header;
                }

                let last_offset =
                    *self.inner.entry_offsets().last().unwrap() as usize;
                let data = &self.inner.data()[last_offset..];
                self.back_header = BlockEntryHeader::decode(
                    &data[..BlockEntryHeader::HEADER_SIZE],
                );
                self.back_key =
                    self.base_key[..self.back_header.overlap as usize].to_vec();
                self.back_key.extend_from_slice(
                    &data[BlockEntryHeader::HEADER_SIZE
                        ..BlockEntryHeader::HEADER_SIZE
                            + self.back_header.diff as usize],
                );
                self.back_entry_index =
                    Some(self.inner.entry_offsets().len() - 1);
                Ok(true)
            }
        }
    }
}
impl KvCacheIter<ValueMeta> for CacheBlockIter {
    fn key(&self) -> Option<KeyTsBorrow<'_>> {
        if self.key.is_empty() {
            return None;
        }
        return Some(self.key.as_slice().into());
    }

    fn value(&self) -> Option<ValueMeta> {
        if let Some(entry_id) = self.entry_index {
            let next_entry_id = entry_id + 1;
            let end_offset =
                if next_entry_id == self.inner.entry_offsets().len() {
                    self.inner.entries_index_start()
                        - self.inner.data_align() as usize
                } else {
                    self.inner.entry_offsets()[next_entry_id] as usize
                };
            let start_offset = self.inner.entry_offsets()[entry_id] as usize
                + BlockEntryHeader::HEADER_SIZE
                + self.header.diff as usize;
            let value = &self.inner.data()[start_offset..end_offset];
            return ValueMeta::decode(value);
        }
        None
    }
}
impl KvDoubleEndedCacheIter<ValueMeta> for CacheBlockIter {
    fn key_back(&self) -> Option<KeyTsBorrow<'_>> {
        if self.back_key.is_empty() {
            return None;
        }
        return Some(self.back_key.as_slice().into());
    }

    fn value_back(&self) -> Option<ValueMeta> {
        if let Some(back_entry_id) = self.back_entry_index {
            let last_entry_id = back_entry_id + 1;
            let end_offset =
                if last_entry_id == self.inner.entry_offsets().len() {
                    self.inner.entries_index_start()
                } else {
                    self.inner.entry_offsets()[last_entry_id] as usize
                };
            let start_offset = self.inner.entry_offsets()[back_entry_id]
                as usize
                + BlockEntryHeader::HEADER_SIZE
                + self.back_header.diff as usize;
            let value = &self.inner.data()[start_offset..end_offset];
            return ValueMeta::decode(value);
        }
        None
    }
}
impl KvSeekIter for CacheBlockIter {
    fn seek(&mut self, k: KeyTsBorrow<'_>) -> Result<bool, IterError> {
        if self.entry_index.is_none() && !self.next()? {
            return Ok(false);
        }

        let search = self.inner.entry_offsets().binary_search_by(|offset| {
            let entry_offset = *offset as usize;
            let data = &self.inner.data()[entry_offset..];
            let header = BlockEntryHeader::decode(
                &data[..BlockEntryHeader::HEADER_SIZE],
            );
            if k.len() >= header.overlap as usize && header.overlap > 8 {
                let split = (header.overlap + header.diff - 8)
                    .min(header.overlap) as usize;
                match self.base_key[..split].cmp(&k[..split]) {
                    std::cmp::Ordering::Equal => {}
                    ord => return ord,
                }
            }
            let mut key =
                vec![0u8; header.overlap as usize + header.diff as usize];
            key[..header.overlap as usize]
                .copy_from_slice(&self.base_key[..header.overlap as usize]);
            key[header.overlap as usize..].copy_from_slice(
                &data[BlockEntryHeader::HEADER_SIZE
                    ..BlockEntryHeader::HEADER_SIZE + header.diff as usize],
            );
            KeyTsBorrow::cmp(&key, &k)
        });

        let entry_index = match search {
            Ok(index) => index,
            Err(index) => {
                if index >= self.inner.entry_offsets().len() {
                    return Ok(false);
                }
                index
            }
        };
        self.set_entry_index(entry_index);
        Ok(true)
    }
}
