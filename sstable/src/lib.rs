mod block;
pub mod cache;
mod error;
mod fb;
mod pb;
mod read;
pub mod table;
mod table_index;
pub mod test_utils;
mod write;
type Result<T> = std::result::Result<T, error::MorsTableError>;
