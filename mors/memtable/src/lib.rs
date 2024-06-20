use mors_traits::file_id::FileId;

mod error;
mod impls;
pub mod memtable;
mod write;

pub(crate) const DEFAULT_DIR: &str = "./tmp/badger";
pub(crate) type Result<T> = std::result::Result<T, error::MorsMemtableError>;
