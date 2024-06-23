use error::MorsError;
mod core;
mod error;
mod test;

pub type Result<T> = std::result::Result<T, MorsError>;
