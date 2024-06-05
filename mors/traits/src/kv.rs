use bytes::Bytes;
use integer_encoding::VarInt;
use lazy_static::lazy_static;

use crate::ts::PhyTs;

pub trait Key {}

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct ValueMeta {
    value: Bytes,
    expires_at: PhyTs,
    user_meta: u8,
    meta: Meta,
}
lazy_static! {
    static ref VALUEMETA_MIN_SERIALIZED_SIZE: usize = ValueMeta::default().serialized_size();
}
impl ValueMeta {
    pub(crate) fn serialized_size(&self) -> usize {
        2 + self.expires_at.required_space() + self.value.len()
    }

    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut v = vec![0u8; self.serialized_size()];
        v[0] = self.user_meta;
        v[1] = self.meta().0;
        let p = self.expires_at.encode_var(&mut v[2..]);
        v[2 + p..].copy_from_slice(self.value());
        v
    }

    pub(crate) fn deserialize(data: &[u8]) -> Option<Self> {
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

    pub(crate) fn meta(&self) -> Meta {
        self.meta
    }

    pub(crate) fn value(&self) -> &Bytes {
        &self.value
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
