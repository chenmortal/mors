use mors_traits::{kms::Kms, ts::KeyTsBorrow};
use bytes::BufMut;
use mors_common::util::Encode;
use mors_traits::kv::ValueMeta;
use prost::Message;

use crate::{block::block_iter::BlockEntryHeader, pb::proto::{checksum::Algorithm, Checksum}};
// const MAX_BUFFER_BLOCK_SIZE: usize = 256 << 20; //256MB
/// When a block is encrypted, it's length increases. We add 256 bytes of padding to
/// handle cases when block size increases. This is an approximate number.
const BLOCK_PADDING: usize = 256;
pub(crate) struct BlockWriter {
    data: Vec<u8>,
    base_keyts: Vec<u8>,
    entry_offsets: Vec<u32>,
}
impl BlockWriter {
    pub(crate) fn new(block_size: usize) -> Self {
        Self {
            data: Vec::with_capacity(block_size + BLOCK_PADDING),
            base_keyts: Default::default(),
            entry_offsets: Default::default(),
        }
    }
    pub(crate) fn entry_offsets(&self) -> &[u32] {
        &self.entry_offsets
    }
    pub(crate) fn data(&self) -> &[u8] {
        &self.data
    }
    pub(crate) fn base_keyts(&self) -> &[u8] {
        &self.base_keyts
    }
    fn diff_base_key(&self, new_key: &[u8]) -> usize {
        let mut i = 0;
        while i < self.base_keyts.len()
            && i < new_key.len()
            && self.base_keyts[i] == new_key[i]
        {
            i += 1;
        }
        i
    }
    pub(crate) fn should_finish_block<K:Kms>(
        &self,
        key: &KeyTsBorrow,
        value: &ValueMeta,
        block_size: usize,
        is_encrypt: bool,
    ) -> bool {
        if self.entry_offsets.is_empty() {
            return false;
        }
        debug_assert!((self.entry_offsets.len() as u32 + 1) * 4 + 4 + 8 + 4 < u32::MAX);
        let entries_offsets_size = (self.entry_offsets.len() + 1) * 4 
        + 4 //size of list
        + 8 //sum64 in checksum proto
        + 4; //checksum length
        let mut estimate_size=self.data.len()+6+key.as_ref().len()+ value.encoded_size() + entries_offsets_size;
        if is_encrypt{
            estimate_size+=K::NONCE_SIZE;
        }
        assert!(self.data.len()+estimate_size < u32::MAX as usize);
        estimate_size > block_size
    }
     fn push_entry(&mut self,key_ts: &KeyTsBorrow,value: &ValueMeta){
        let diff_key=if self.base_keyts.is_empty() {
            self.base_keyts=key_ts.to_vec();
            key_ts
        }else{
            &key_ts[self.diff_base_key(key_ts)..]
        };
        assert!(key_ts.len()-diff_key.len() <= u16::MAX as usize);
        assert!(diff_key.len() <= u16::MAX as usize);
        let entry_header=BlockEntryHeader::new((key_ts.len()-diff_key.len()) as u16, diff_key.len() as u16);
        self.entry_offsets.push(self.data.len() as u32);
        self.data.extend_from_slice(&entry_header.encode());
        self.data.extend_from_slice(diff_key);
        self.data.extend_from_slice(value.encode().as_ref());
        
    }
    pub(crate) fn finish_block(&mut self,algo:Algorithm){
        self.data.extend_from_slice(&self.entry_offsets.encode());
        self.data.put_u32(self.entry_offsets.len() as u32);

        let checksum = Checksum::new(algo, &self.data);
        self.data.extend_from_slice(&checksum.encode_to_vec());
        self.data.put_u32(checksum.encoded_len() as u32);
    }
}