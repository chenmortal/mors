use error::MorsVlogError;

pub mod vlogctl;
pub mod error;
pub mod discard;
pub mod write;
mod threshold;

type Result<T> = std::result::Result<T, MorsVlogError>;