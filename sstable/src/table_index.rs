use std::sync::Arc;

// use flatbuffers::InvalidFlatbuffer;
use mors_traits::sstable::TableIndexBufTrait;
use serde::{Deserialize, Serialize};

// use crate::fb::table_generated::TableIndex;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TableIndexBuf(Arc<TableIndexBufInner>);
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct TableIndexBufInner {
    offsets: Vec<BlockOffset>,
    bloom_filter: Option<Vec<u8>>,
    offsets_len: usize,
    max_version: u64,
    key_count: u32,
    uncompressed_size: u32,
    on_disk_size: u32,
    stale_data_size: u32,
}
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct BlockOffset {
    key_ts: Vec<u8>,
    offset: u32,
    len: u32,
}
impl BlockOffset {
    pub fn new(key_ts: Vec<u8>, offset: u32, len: u32) -> Self {
        Self {
            key_ts,
            offset,
            len,
        }
    }
    pub fn key_ts(&self) -> &[u8] {
        &self.key_ts
    }
    pub fn offset(&self) -> u32 {
        self.offset
    }
    pub fn len(&self) -> u32 {
        self.len
    }
}
impl TableIndexBufTrait for TableIndexBuf {}
impl TableIndexBufInner {
    pub fn new(
        offsets: Vec<BlockOffset>,
        bloom_filter: Option<Vec<u8>>,
        max_version: u64,
        key_count: u32,
        uncompressed_size: u32,
        on_disk_size: u32,
        stale_data_size: u32,
    ) -> Self {
        Self {
            offsets_len: offsets.len(),
            offsets,
            bloom_filter,
            max_version,
            key_count,
            uncompressed_size,
            on_disk_size,
            stale_data_size,
        }
    }
}
impl TableIndexBuf {
    pub(crate) fn from_vec(data: Vec<u8>) -> Self {
        let data = bincode::deserialize::<TableIndexBufInner>(&data).unwrap();
        Self(Arc::new(data))
    }
    // pub(crate) fn from_vec(data: Vec<u8>) -> Result<Self, InvalidFlatbuffer> {
    //     let table_index =
    //         unsafe { flatbuffers::root_unchecked::<TableIndex>(&data) };

    //     assert!(table_index.offsets().is_some());
    //     let offsets = table_index.offsets().unwrap();
    //     let offsets_len = offsets.len();
    //     Ok(Self(
    //         TableIndexBufInner {
    //             max_version: table_index.max_version(),
    //             key_count: table_index.key_count(),
    //             uncompressed_size: table_index.uncompressed_size(),
    //             on_disk_size: table_index.on_disk_size(),
    //             stale_data_size: table_index.stale_data_size(),
    //             data,
    //             offsets_len,
    //         }
    //         .into(),
    //     ))
    // }
    pub(crate) fn offsets(&self) -> &[BlockOffset] {
        &self.0.offsets
    }
    // pub(crate) fn offsets(&self) -> &[BlockOffsetBuf] {
    //     &self.0.offsets
    // }
    // pub(crate) fn offsets(
    //     &self,
    // ) -> Vector<'_, ForwardsUOffset<BlockOffset<'_>>> {
    //     let table_index =
    //         unsafe { flatbuffers::root_unchecked::<TableIndex>(&self.0.data) };
    //     let k = table_index.offsets().unwrap();
    //     k
    //     // table_index.offsets().unwrap().bytes()
    // }
    pub(crate) fn offsets_len(&self) -> usize {
        self.0.offsets_len
    }
    pub(crate) fn bloom_filter(&self) -> Option<&[u8]> {
        self.0.bloom_filter.as_deref()
        // let table_index =
        //     unsafe { flatbuffers::root_unchecked::<TableIndex>(&self.0.data) };
        // table_index.bloom_filter().map(|x| x.bytes())
    }
    pub(crate) fn max_version(&self) -> u64 {
        self.0.max_version
    }
    pub(crate) fn key_count(&self) -> u32 {
        self.0.key_count
    }
    pub(crate) fn uncompressed_size(&self) -> u32 {
        self.0.uncompressed_size
    }
    pub(crate) fn on_disk_size(&self) -> u32 {
        self.0.on_disk_size
    }
    pub(crate) fn stale_data_size(&self) -> u32 {
        self.0.stale_data_size
    }
}
