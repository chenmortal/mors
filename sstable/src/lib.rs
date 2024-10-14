#[allow(dead_code)]
mod block;
pub mod cache;
mod checksum;
mod error;
mod read;
#[allow(dead_code)]
pub mod table;
mod table_index;
pub mod test_utils;
#[allow(dead_code)]
mod write;
type Result<T> = std::result::Result<T, error::MorsTableError>;
