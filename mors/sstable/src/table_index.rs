use bytes::Bytes;
use flatbuffers::InvalidFlatbuffer;
use mors_traits::{sstable::TableIndexBufTrait, ts::KeyTs};

use crate::fb::table_generated::TableIndex;

#[derive(Clone, Debug, Default)]
pub struct TableIndexBuf {
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
        let table_index = flatbuffers::root::<TableIndex>(&data)?;
        assert!(table_index.offsets().is_some());
        let offsets = table_index.offsets().unwrap();
        let offsets = offsets
            .iter()
            .map(|offset| {
                assert!(offset.key_ts().is_some());
                BlockOffsetBuf {
                    key_ts: KeyTs::from(offset.key_ts().unwrap().bytes()),
                    offset: offset.offset(),
                    size: offset.len(),
                }
            })
            .collect::<Vec<_>>();

        let bloom_filter = table_index
            .bloom_filter()
            .and_then(|x| Bytes::from(x.bytes().to_vec()).into());
        Ok(Self {
            offsets,
            bloom_filter,
            max_version: table_index.max_version(),
            key_count: table_index.key_count(),
            uncompressed_size: table_index.uncompressed_size(),
            on_disk_size: table_index.on_disk_size(),
            stale_data_size: table_index.stale_data_size(),
        })
    }
    pub(crate) fn offsets(&self) -> &[BlockOffsetBuf] {
        &self.offsets
    }
}
#[derive(Debug, Clone)]
pub(crate) struct BlockOffsetBuf {
    key_ts: KeyTs,
    offset: u32,
    size: u32,
}
impl BlockOffsetBuf {
    pub(crate) fn key_ts(&self) -> &KeyTs {
        &self.key_ts
    }
    pub(crate) fn offset(&self) -> u32 {
        self.offset
    }
    pub(crate) fn size(&self) -> u32 {
        self.size
    }
}
