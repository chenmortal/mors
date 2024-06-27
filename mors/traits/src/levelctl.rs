use std::{
    fmt::Display,
    ops::{Add, AddAssign, Sub},
};

use crate::{kms::Kms, sstable::TableTrait};

pub trait LevelCtl<T:TableTrait>: Sized {
    type ErrorType;
    type LevelCtlBuilder: LevelCtlBuilder<Self,T>;
}
pub trait LevelCtlBuilder<L:LevelCtl<T>,T:TableTrait>: Default {
    fn build(&self,kms:impl Kms)->impl std::future::Future<Output = Result<(),L::ErrorType>>;
}

pub const LEVEL0: Level = Level(0);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Level(u8);
impl From<u8> for Level {
    fn from(value: u8) -> Self {
        Self(value)
    }
}
impl From<u32> for Level {
    fn from(value: u32) -> Self {
        Self(value as u8)
    }
}
impl From<usize> for Level {
    fn from(value: usize) -> Self {
        Self(value as u8)
    }
}
impl From<Level> for usize {
    fn from(val: Level) -> Self {
        val.0 as usize
    }
}
impl From<Level> for u32 {
    fn from(val: Level) -> Self {
        val.0 as u32
    }
}
impl Level {
    pub fn to_usize(&self) -> usize {
        self.0 as usize
    }
    pub fn to_u8(&self) -> u8 {
        self.0
    }
}
impl Add<u8> for Level {
    type Output = Self;

    fn add(self, rhs: u8) -> Self::Output {
        Self(self.0 + rhs)
    }
}
impl Sub<u8> for Level {
    type Output = Self;

    fn sub(self, rhs: u8) -> Self::Output {
        Self(self.0 - rhs)
    }
}
impl AddAssign<u8> for Level {
    fn add_assign(&mut self, rhs: u8) {
        self.0 += rhs
    }
}

impl Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Level {}", self.0))
    }
}
#[test]
fn test_level_step() {
    let start: u8 = 0;
    let end = 5;
    let start_l: Level = start.into();
    let end_l: Level = end.into();
    let mut step = start;
    for l in start_l.to_u8()..end_l.to_u8() {
        assert_eq!(l, step);
        step += 1;
    }
    assert_eq!(step, end);
}
