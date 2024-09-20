use std::sync::Arc;

use bytes::Bytes;
use flatbuffers::InvalidFlatbuffer;
use mors_common::ts::KeyTsBorrow;
use mors_traits::sstable::TableIndexBufTrait;

use crate::fb::table_generated::TableIndex;

#[derive(Clone, Debug, Default)]
pub struct TableIndexBuf(Arc<TableIndexBufInner>);
#[derive(Debug, Default)]
struct TableIndexBufInner {
    offsets: Vec<BlockOffsetBuf>,
    bloom_filter: Option<Bytes>,
    max_version: u64,
    key_count: u32,
    uncompressed_size: u32,
    on_disk_size: u32,
    stale_data_size: u32,
}
impl TableIndexBufTrait for TableIndexBuf {}

impl TableIndexBuf {
    pub(crate) fn from_vec(data: Vec<u8>) -> Result<Self, InvalidFlatbuffer> {
        let table_index =
            unsafe { flatbuffers::root_unchecked::<TableIndex>(&data) };
        assert!(table_index.offsets().is_some());
        let offsets = table_index.offsets().unwrap();
        let offsets = offsets
            .iter()
            .map(|offset| {
                assert!(offset.key_ts().is_some());
                offsets.bytes().to_vec();
                BlockOffsetBuf {
                    key_ts: Bytes::from(
                        offset.key_ts().unwrap().bytes().to_vec(),
                    ),
                    offset: offset.offset(),
                    size: offset.len(),
                }
            })
            .collect::<Vec<_>>();

        let bloom_filter = table_index
            .bloom_filter()
            .and_then(|x| Bytes::from(x.bytes().to_vec()).into());
        Ok(Self(
            TableIndexBufInner {
                offsets,
                bloom_filter,
                max_version: table_index.max_version(),
                key_count: table_index.key_count(),
                uncompressed_size: table_index.uncompressed_size(),
                on_disk_size: table_index.on_disk_size(),
                stale_data_size: table_index.stale_data_size(),
            }
            .into(),
        ))
    }
    pub(crate) fn offsets(&self) -> &[BlockOffsetBuf] {
        &self.0.offsets
    }
    pub(crate) fn bloom_filter(&self) -> Option<&Bytes> {
        self.0.bloom_filter.as_ref()
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
#[derive(Debug, Clone)]
pub(crate) struct BlockOffsetBuf {
    key_ts: Bytes,
    offset: u32,
    size: u32,
}
impl BlockOffsetBuf {
    pub(crate) fn key_ts(&self) -> KeyTsBorrow {
        KeyTsBorrow::from(self.key_ts.as_ref())
    }
    pub(crate) fn offset(&self) -> u32 {
        self.offset
    }
    pub(crate) fn size(&self) -> u32 {
        self.size
    }
}
