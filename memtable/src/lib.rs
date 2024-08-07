mod error;
pub mod memtable;
mod write;
mod read;
mod impls;
pub(crate) type Result<T> = std::result::Result<T, error::MorsMemtableError>;
