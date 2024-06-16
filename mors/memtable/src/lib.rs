use mors_traits::file_id::FileId;

mod error;
pub mod memtable;
mod write;

pub(crate) const DEFAULT_DIR: &str = "./tmp/badger";
pub(crate) type Result<T> = std::result::Result<T, error::MorsMemtableError>;
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MorsMemtableId(u32);
impl From<u32> for MorsMemtableId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl From<MorsMemtableId> for u32 {
    fn from(value: MorsMemtableId) -> Self {
        value.0
    }
}
impl FileId for MorsMemtableId {
    const SUFFIX: &'static str = ".mem";
}
