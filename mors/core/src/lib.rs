use error::MorsError;
pub mod core;
mod error;
mod test;

pub type Result<T> = std::result::Result<T, MorsError>;
