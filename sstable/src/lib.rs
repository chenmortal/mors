mod error;
mod pb;
pub mod table;
mod block;
mod block_iter;
mod fb;
mod cache;
mod table_index;
type Result<T> = std::result::Result<T, error::MorsTableError>;
