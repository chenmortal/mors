mod read;
pub mod write;
use std::sync::Arc;

use bytes::Buf;
use mors_common::{file_id::SSTableId, util::BufExt};
use mors_traits::sstable::{BlockIndex, BlockTrait};
use prost::Message;
use read::CacheBlockIter;

use crate::{error::MorsTableError, pb::proto::Checksum, Result};
#[derive(Default, Clone)]
pub struct Block(Arc<BlockInner>);
#[derive(Default)]
struct BlockInner {
    table_id: SSTableId,
    block_index: BlockIndex,
    block_offset: u32,
    data: Vec<u8>, //actual data + entry_offsets+num_entries;
    entries_index_start: usize,
    entry_offsets: Vec<u32>,
    checksum: Vec<u8>,
    checksum_len: usize,
}
impl Block {
    pub(crate) fn decode(
        table_id: SSTableId,
        block_index: BlockIndex,
        block_offset: u32,
        mut data: Vec<u8>,
    ) -> Result<Self> {
        //read checksum len
        let mut read_pos = data.len() - 4;
        let mut checksum_len = &data[read_pos..read_pos + 4];
        let checksum_len = checksum_len.get_u32() as usize;

        if checksum_len > data.len() {
            return Err(MorsTableError::InvalidChecksumLen);
        }

        //read checksum
        read_pos -= checksum_len;
        let checksum = data[read_pos..read_pos + checksum_len].to_vec();
        data.truncate(read_pos);

        //read num entries
        read_pos -= 4;
        let mut num_entries = &data[read_pos..read_pos + 4];
        let num_entries = num_entries.get_u32() as usize;

        //read entries index start
        let entries_index_start = read_pos - (num_entries * 4);
        let mut entries = &data[entries_index_start..read_pos];
        let entry_offsets = entries.get_vec_u32();
        Ok(Block(Arc::new(BlockInner {
            table_id,
            block_index,
            block_offset,
            data,
            entries_index_start,
            entry_offsets,
            checksum,
            checksum_len,
        })))
    }
    pub(crate) fn verify(&self) -> Result<()> {
        let checksum = Checksum::decode(self.0.checksum.as_ref())?;
        checksum.verify(&self.0.data)?;
        Ok(())
    }
    pub(crate) fn data(&self) -> &[u8] {
        &self.0.data
    }
    pub(crate) fn block_index(&self) -> BlockIndex {
        self.0.block_index
    }
    pub(crate) fn table_id(&self) -> SSTableId {
        self.0.table_id
    }
    pub(crate) fn block_offset(&self) -> u32 {
        self.0.block_offset
    }
    pub(crate) fn entry_offsets(&self) -> &[u32] {
        &self.0.entry_offsets
    }
    pub(crate) fn entries_index_start(&self) -> usize {
        self.0.entries_index_start
    }
    pub(crate) fn checksum(&self) -> &[u8] {
        &self.0.checksum
    }
    pub(crate) fn checksum_len(&self) -> usize {
        self.0.checksum_len
    }
    pub(crate) fn iter(&self) -> CacheBlockIter {
        self.clone().into()
    }
}
impl BlockTrait for Block {}
