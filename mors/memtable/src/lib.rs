mod error;
pub mod memtable;
mod write;

pub(crate) const DEFAULT_DIR: &str = "./tmp/badger";
pub(crate) type Result<T> = std::result::Result<T, error::MemtableError>;
