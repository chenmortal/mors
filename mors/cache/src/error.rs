use thiserror::Error;
#[derive(Error, Debug)]
pub enum MorsCacheError {
    #[error("IO Error: {0}")]
    IOErr(#[from] std::io::Error),

}