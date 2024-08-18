use bytes::{Buf, BufMut, Bytes};
use integer_encoding::VarInt;
use lazy_static::lazy_static;

use crate::{
    file_id::FileId,
    ts::{KeyTs, PhyTs, TxnTs},
};

pub trait Key {}
#[derive(Debug, Default, Clone)]
pub struct Entry {
    key_ts: KeyTs,
    value_meta: ValueMeta,
    offset: usize,
    value_threshold: usize,
}

impl Entry {
    pub fn new(key: Bytes, value: Bytes) -> Self {
        let key_ts = KeyTs::new(key, TxnTs::default());
        let value_meta = ValueMeta {
            value,
            expires_at: PhyTs::default(),
            user_meta: 0,
            meta: Meta::default(),
        };
        Self {
            key_ts,
            offset: 0,
            value_meta,
            value_threshold: 0,
        }
    }
    #[inline]
    pub fn from_log(key_ts: &[u8], value: &[u8], offset: usize) -> Self {
        let k: KeyTs = key_ts.into();
        let value_meta = ValueMeta {
            value: value.to_vec().into(),
            expires_at: PhyTs::default(),
            user_meta: 0,
            meta: Meta::default(),
        };
        Self {
            key_ts: k,
            offset,
            value_meta,
            value_threshold: 0,
        }
    }
    pub fn key_ts(&self) -> &KeyTs {
        &self.key_ts
    }
    pub fn value_meta(&self) -> &ValueMeta {
        &self.value_meta
    }
    pub fn value_meta_mut(&mut self) -> &mut ValueMeta {
        &mut self.value_meta
    }
    pub fn value(&self) -> &Bytes {
        &self.value_meta.value
    }
    pub fn set_value<B: Into<Bytes>>(&mut self, value: B) -> &mut Self {
        self.value_meta.set_value(value.into());
        self
    }
    pub fn meta(&self) -> Meta {
        self.value_meta.meta()
    }
    pub fn set_meta(&mut self, meta: Meta) -> &mut Self {
        self.value_meta.meta = meta;
        self
    }
    pub fn meta_mut(&mut self) -> &mut Meta {
        &mut self.value_meta.meta
    }
    pub fn set_user_meta(&mut self, user_meta: u8) -> &mut Self {
        self.value_meta.user_meta = user_meta;
        self
    }
    pub fn user_meta(&self) -> u8 {
        self.value_meta.user_meta()
    }
    pub fn expires_at(&self) -> PhyTs {
        self.value_meta.expires_at()
    }
    pub fn version(&self) -> TxnTs {
        self.key_ts.txn_ts()
    }
    pub fn set_version(&mut self, txn_ts: TxnTs) -> &mut Self {
        self.key_ts.set_txn_ts(txn_ts);
        self
    }
    pub fn offset(&self) -> usize {
        self.offset
    }
    pub fn value_threshold(&self) -> usize {
        self.value_threshold
    }
    pub fn set_value_threshold(&mut self, value_threshold: usize) -> &mut Self {
        self.value_threshold = value_threshold;
        self
    }
}

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ValuePointer {
    fid: u32,
    size: u32,
    offset: u64,
}
impl ValuePointer {
    pub fn new<I: FileId>(file_id: I, size: u32, offset: u64) -> Self {
        Self {
            fid: file_id.into(),
            size,
            offset,
        }
    }
    pub fn fid(&self) -> u32 {
        self.fid
    }
    pub fn size(&self) -> u32 {
        self.size
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(16);
        v.put_u32(self.fid);
        v.put_u32(self.size);
        v.put_u64(self.offset);
        v
    }
    pub fn decode(mut data: &[u8]) -> Option<Self> {
        if data.len() < 16 {
            return None;
        }
        let fid = data.get_u32();
        let size = data.get_u32();
        let offset = data.get_u64();
        Some(Self { fid, size, offset })
    }
    pub fn is_empty(&self) -> bool {
        *self == ValuePointer::default()
    }
}
#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct ValueMeta {
    value: Bytes,
    expires_at: PhyTs,
    user_meta: u8,
    meta: Meta,
}
lazy_static! {
    static ref VALUEMETA_MIN_ENCODED_SIZE: usize =
        ValueMeta::default().encoded_size();
}
impl ValueMeta {
    pub fn encoded_size(&self) -> usize {
        2 + self.expires_at.required_space() + self.value.len()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut v = vec![0u8; self.encoded_size()];
        v[0] = self.user_meta;
        v[1] = self.meta().0;
        let p = self.expires_at.encode_var(&mut v[2..]);
        v[2 + p..].copy_from_slice(self.value());
        v
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < VALUEMETA_MIN_ENCODED_SIZE.to_owned() {
            return None;
        }
        if let Some((expires_at, size)) = u64::decode_var(&data[2..]) {
            return Self {
                value: data[2 + size..].to_vec().into(),
                expires_at: expires_at.into(),
                user_meta: data[0],
                meta: Meta(data[1]),
            }
            .into();
        }
        None
    }
    pub fn value(&self) -> &Bytes {
        &self.value
    }
    pub fn set_value(&mut self, value: Bytes) {
        self.value = value;
    }
    pub fn is_deleted_or_expired(&self) -> bool {
        if self.meta.contains(Meta::DELETE) {
            return true;
        };
        if self.expires_at == PhyTs::default() {
            return false;
        }
        self.expires_at <= PhyTs::now().unwrap()
    }
    pub fn set_expires_at(&mut self, expires_at: PhyTs) {
        self.expires_at = expires_at;
    }
    pub fn expires_at(&self) -> PhyTs {
        self.expires_at
    }
    pub fn user_meta(&self) -> u8 {
        self.user_meta
    }
    pub fn set_user_meta(&mut self, user_meta: u8) {
        self.user_meta = user_meta;
    }
    pub fn meta(&self) -> Meta {
        self.meta
    }
    pub fn set_meta(&mut self, meta: Meta) {
        self.meta = meta;
    }
}
#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Meta(u8);
bitflags::bitflags! {
    impl Meta: u8 {
        const DELETE = 1<<0;
        const VALUE_POINTER = 1 << 1;
        const DISCARD_EARLIER_VERSIONS = 1 << 2;
        const MERGE_ENTRY=1<<3;
        const TXN=1<<6;
        const FIN_TXN=1<<7;
    }
}
impl std::fmt::Debug for Meta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        bitflags::parser::to_writer(self, f)
    }
}
impl std::fmt::Display for Meta {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        bitflags::parser::to_writer(self, f)
    }
}
