use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::ops::{Add, AddAssign, Deref, Sub};
use std::time::{Duration, SystemTime, SystemTimeError};

use bytes::{Buf, BufMut, Bytes};
use pretty_hex::PrettyHex;

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct PhyTs(u64);
impl Deref for PhyTs {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl From<u64> for PhyTs {
    fn from(value: u64) -> Self {
        Self(value)
    }
}
impl From<PhyTs> for u64 {
    fn from(value: PhyTs) -> Self {
        value.0
    }
}
impl From<SystemTime> for PhyTs {
    fn from(value: SystemTime) -> Self {
        value
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .into()
    }
}

impl From<PhyTs> for SystemTime {
    fn from(value: PhyTs) -> Self {
        SystemTime::UNIX_EPOCH.add(Duration::from_secs(value.0))
    }
}

impl PhyTs {
    pub fn now() -> Result<Self, SystemTimeError> {
        Ok(SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs()
            .into())
    }
    pub fn to_u64(&self) -> u64 {
        self.0
    }
}

///this means TransactionTimestamp
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnTs(u64);
impl TxnTs {
    #[inline(always)]
    pub fn to_u64(&self) -> u64 {
        self.0
    }
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }
}
impl Add<u64> for TxnTs {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        (self.0 + rhs).into()
    }
}
impl Sub<u64> for TxnTs {
    type Output = Self;

    fn sub(self, rhs: u64) -> Self::Output {
        (self.0 - rhs).into()
    }
}
impl AddAssign<u64> for TxnTs {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}
impl Display for TxnTs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("TxnTs({})", self.0))
    }
}
impl From<u64> for TxnTs {
    fn from(value: u64) -> Self {
        Self(value)
    }
}
impl From<TxnTs> for u64 {
    fn from(value: TxnTs) -> Self {
        value.0
    }
}
#[derive(PartialEq, Eq, Clone)]
pub struct KeyTs {
    key: Bytes,
    txn_ts: TxnTs,
    to_string: fn(&[u8]) -> String,
}
impl Default for KeyTs {
    fn default() -> Self {
        Self {
            key: Bytes::default(),
            txn_ts: TxnTs::default(),
            to_string: |x| x.hex_dump().to_string(),
        }
    }
}
impl Debug for KeyTs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "KeyTs: Key({})-{}",
            (self.to_string)(&self.key),
            self.txn_ts
        ))
    }
}
impl Display for KeyTs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "KeyTs: Key({})-{}",
            (self.to_string)(&self.key),
            self.txn_ts
        ))
    }
}
impl KeyTs {
    pub fn new(key: Bytes, txn_ts: TxnTs) -> Self {
        Self {
            key,
            txn_ts,
            ..Default::default()
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.key.len() + 8);
        v.put_slice(&self.key);
        v.put_u64(self.txn_ts.to_u64());
        v
    }

    pub fn key(&self) -> &Bytes {
        &self.key
    }

    pub fn txn_ts(&self) -> TxnTs {
        self.txn_ts
    }
    pub fn set_to_string(&mut self, to_string: fn(&[u8]) -> String) {
        self.to_string = to_string;
    }
    pub fn set_key(&mut self, key: Bytes) {
        self.key = key;
    }

    pub fn set_txn_ts(&mut self, txn_ts: TxnTs) {
        self.txn_ts = txn_ts;
    }

    pub fn len(&self) -> usize {
        self.key.len() + std::mem::size_of::<u64>()
    }

    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}
impl From<KeyTsBorrow<'_>> for KeyTs {
    fn from(value: KeyTsBorrow<'_>) -> Self {
        value.as_ref().into()
    }
}
impl PartialOrd for KeyTs {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq<KeyTsBorrow<'_>> for KeyTs {
    fn eq(&self, other: &KeyTsBorrow<'_>) -> bool {
        self.key == other.key() && self.txn_ts() == other.txn_ts()
    }
}
impl PartialOrd<KeyTsBorrow<'_>> for KeyTs {
    fn partial_cmp(&self, other: &KeyTsBorrow<'_>) -> Option<Ordering> {
        match self.key().partial_cmp(other.key()) {
            Some(Ordering::Equal) => {}
            ord => {
                return ord;
            }
        };
        other.txn_ts().partial_cmp(&self.txn_ts())
    }
}

impl Ord for KeyTs {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => {}
            ord => return ord,
        }
        other.txn_ts.cmp(&self.txn_ts)
    }
}
impl From<&[u8]> for KeyTs {
    fn from(value: &[u8]) -> Self {
        let len = value.len();
        if len <= 8 {
            Self {
                key: value.to_vec().into(),
                txn_ts: 0.into(),
                ..Default::default()
            }
        } else {
            let mut p = &value[len - 8..];
            Self {
                key: value[..len - 8].to_vec().into(),
                txn_ts: p.get_u64().into(),
                ..Default::default()
            }
        }
    }
}
#[derive(PartialEq, Eq, Clone, Copy, Default)]
pub struct KeyTsBorrow<'a>(&'a [u8]);
impl<'a> KeyTsBorrow<'a> {
    pub fn key(&self) -> &[u8] {
        if self.len() >= 8 {
            &self[..self.len() - 8]
        } else {
            &self[..]
        }
    }
    pub fn txn_ts(&self) -> TxnTs {
        if self.len() >= 8 {
            let mut p = &self[self.len() - 8..];
            p.get_u64().into()
        } else {
            TxnTs::default()
        }
    }
    pub fn is_empty(&self) -> bool {
        self.key().is_empty()
    }
}

impl Deref for KeyTsBorrow<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}
impl Debug for KeyTsBorrow<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "KeyTs: Key({})-{}",
            self.key().hex_dump(),
            self.txn_ts()
        ))
    }
}
impl Display for KeyTsBorrow<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "KeyTs: Key({})-{}",
            self.key().hex_dump(),
            self.txn_ts()
        ))
    }
}
impl Ord for KeyTsBorrow<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key().cmp(other.key()) {
            Ordering::Equal => {}
            ord => {
                return ord;
            }
        }
        other.txn_ts().cmp(&self.txn_ts())
    }
}
impl PartialOrd for KeyTsBorrow<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq<KeyTs> for KeyTsBorrow<'_> {
    fn eq(&self, other: &KeyTs) -> bool {
        self.key() == other.key() && self.txn_ts() == other.txn_ts()
    }
}

impl PartialOrd<KeyTs> for KeyTsBorrow<'_> {
    fn partial_cmp(&self, other: &KeyTs) -> Option<Ordering> {
        match self.key().partial_cmp(other.key()) {
            Some(Ordering::Equal) => {}
            ord => {
                return ord;
            }
        }
        other.txn_ts().partial_cmp(&self.txn_ts())
    }
}
impl KeyTsBorrow<'_> {
    pub fn cmp(left: &[u8], right: &[u8]) -> Ordering {
        if left.len() > 8 && right.len() > 8 {
            let left_split = left.len() - 8;
            let right_split = right.len() - 8;
            match left[..left_split].cmp(&right[..right_split]) {
                Ordering::Equal => {}
                ord => {
                    return ord;
                }
            }
            right[right_split..].cmp(&left[left_split..])
        } else {
            left.cmp(right)
        }
    }
    // pub(crate) fn equal_key(left: &[u8], right: &[u8]) -> bool {
    //     if left.len() > 8 && right.len() > 8 {
    //         let left_split = left.len() - 8;
    //         let right_split = right.len() - 8;
    //         left[..left_split] == right[..right_split]
    //     } else {
    //         left == right
    //     }
    // }
}
impl<'a> From<&'a [u8]> for KeyTsBorrow<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self(value)
    }
}
impl<'a> AsRef<[u8]> for KeyTsBorrow<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}
impl<'a> From<KeyTsBorrow<'a>> for &'a [u8] {
    fn from(val: KeyTsBorrow<'a>) -> Self {
        val.0
    }
}
#[cfg(test)]
mod tests {
    use crate::ts::KeyTs;

    #[test]
    fn test_fmt() {
        let mut key_ts = KeyTs::new("hello".into(), 10.into());
        key_ts.set_to_string(|x| String::from_utf8_lossy(x).to_string());
        assert_eq!(format!("{:?}", key_ts), "KeyTs: Key(hello)-TxnTs(10)");
    }
}
