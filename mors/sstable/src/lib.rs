mod error;
mod pb;
mod table;
mod block;
mod block_iter;
mod fb;
mod table_index;
type Result<T> = std::result::Result<T, error::MorsTableError>;
