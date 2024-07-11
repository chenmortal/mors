extern crate thiserror;


use error::MorsSkipListError;



pub mod arena;
mod error;
pub mod impls;
mod iter;
mod skip_list;
pub(crate) type Result<T> = std::result::Result<T, MorsSkipListError>;


