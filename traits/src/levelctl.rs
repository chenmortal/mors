use crate::default::{WithDir, WithReadOnly};
use crate::vlog::DiscardTrait;
use crate::{kms::Kms, sstable::TableTrait};
use mors_common::closer::Closer;
use mors_common::kv::ValueMeta;
use mors_common::ts::{KeyTs, TxnTs};
use std::error::Error;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::{
    fmt::Display,
    ops::{Add, AddAssign, Sub},
};
use thiserror::Error;

pub trait LevelCtlTrait<T: TableTrait<K::Cipher>, K: Kms>:
    Sized + Send + Sync + Clone + 'static
{
    type ErrorType: Into<LevelCtlError>;
    type LevelCtlBuilder: LevelCtlBuilderTrait<Self, T, K>;
    fn max_version(&self) -> TxnTs;
    fn table_builder(&self) -> &T::TableBuilder;
    fn next_id(&self) -> Arc<AtomicU32>;
    fn push_level0(
        &self,
        table: T,
    ) -> impl std::future::Future<Output = Result<(), LevelCtlError>> + Send;
    fn get(
        &self,
        key: &KeyTs,
    ) -> impl std::future::Future<
        Output = Result<Option<(TxnTs, Option<ValueMeta>)>, LevelCtlError>,
    > + Send;
    fn spawn_compact<D: DiscardTrait>(
        self,
        closer: Closer,
        kms: K,
        discard: D,
    ) -> impl std::future::Future<Output = ()> + Send;
}
pub trait LevelCtlBuilderTrait<
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
    K: Kms,
>: Default + WithDir + WithReadOnly
{
    fn build(
        &self,
        kms: K,
    ) -> impl std::future::Future<Output = Result<L, LevelCtlError>>;
    fn set_cache(&mut self, cache: T::Cache) -> &mut Self;
    fn set_level0_table_size(&mut self, size: usize) -> &mut Self;
}
#[derive(Error, Debug)]
pub struct LevelCtlError(Box<dyn Error>);
impl LevelCtlError {
    pub fn new<E: Error + 'static>(error: E) -> Self {
        LevelCtlError(Box::new(error))
    }
}
impl Display for LevelCtlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LevelCtlError: {}", self.0)
    }
}
unsafe impl Send for LevelCtlError {}
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
