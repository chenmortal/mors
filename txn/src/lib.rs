use error::MorsTxnError;

mod error;
pub mod manager;
mod mark;
mod txn;
pub(crate) type Result<T> = std::result::Result<T, MorsTxnError>;
