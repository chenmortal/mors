use std::ops::{Add, AddAssign};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CipherKeyId(u64);
impl From<u64> for CipherKeyId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}
impl From<CipherKeyId> for u64 {
    fn from(value: CipherKeyId) -> Self {
        value.0
    }
}

impl Add<u64> for CipherKeyId {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        (self.0 + rhs).into()
    }
}
impl AddAssign<u64> for CipherKeyId {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs
    }
}
