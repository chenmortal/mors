mod error;
mod pb;
pub mod table;
mod block;
mod fb;
mod cache;
mod table_index;
mod write;
type Result<T> = std::result::Result<T, error::MorsTableError>;
