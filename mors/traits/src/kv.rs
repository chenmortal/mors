use bytes::Bytes;
use integer_encoding::VarInt;
use lazy_static::lazy_static;

use crate::{file_id::FileId,  ts::{KeyTs, PhyTs, TxnTs}};

pub trait Key {}
#[derive(Debug, Default, Clone)]
pub struct Entry {
    key_ts: KeyTs,
    value_meta: ValueMeta,
    offset: usize,
    header_len: usize,
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
            header_len: 0,
            value_meta,
            value_threshold: 0,
        }
    }
    #[inline]
    pub fn new_ts(
        key_ts: &[u8],
        value: &[u8],
        offset: usize,
        header_len: usize,
    ) -> Self {
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
            header_len,
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
    pub fn meta(&self) -> Meta {
        self.value_meta.meta()
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
    pub fn offset(&self) -> usize {
        self.offset
    }
    pub fn header_len(&self) -> usize {
        self.header_len
    }
    pub fn value_threshold(&self) -> usize {
        self.value_threshold
    }
}


#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ValuePointer {
    fid: u32,
    len: u32,
    offset: u64,
}
impl ValuePointer {
    pub fn new<I:FileId>(file_id: I, len: u32, offset: u64) -> Self {
        Self { fid:file_id.into(), len, offset }
    }
    pub fn fid(&self) -> u32 {
        self.fid
    }
    pub fn len(&self) -> u32 {
        self.len
    }
    pub fn offset(&self) -> u64 {
        self.offset
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
    static ref VALUEMETA_MIN_SERIALIZED_SIZE: usize =
        ValueMeta::default().serialized_size();
}
impl ValueMeta {
    pub fn serialized_size(&self) -> usize {
        2 + self.expires_at.required_space() + self.value.len()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut v = vec![0u8; self.serialized_size()];
        v[0] = self.user_meta;
        v[1] = self.meta().0;
        let p = self.expires_at.encode_var(&mut v[2..]);
        v[2 + p..].copy_from_slice(self.value());
        v
    }

    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < VALUEMETA_MIN_SERIALIZED_SIZE.to_owned() {
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

    pub(crate) fn set_value(&mut self, value: Bytes) {
        self.value = value;
    }
    pub(crate) fn is_deleted_or_expired(&self) -> bool {
        if self.meta.contains(Meta::DELETE) {
            return true;
        };
        if self.expires_at == PhyTs::default() {
            return false;
        }
        self.expires_at <= PhyTs::now().unwrap()
    }
    pub fn value(&self) -> &Bytes {
        &self.value
    }
    pub fn expires_at(&self) -> PhyTs {
        self.expires_at
    }
    pub fn user_meta(&self) -> u8 {
        self.user_meta
    }
    pub fn meta(&self) -> Meta {
        self.meta
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
