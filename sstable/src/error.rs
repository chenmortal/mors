use mors_common::compress::CompressError;
use mors_traits::{iter::IterError, kms::EncryptError, sstable::SSTableError};
use prost::DecodeError;
use thiserror::Error;
#[derive(Error, Debug)]
pub enum MorsTableError {
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("compression and block_size cannot be both empty")]
    InvalidConfig,
    #[error("decode error: {0}")]
    DecodeError(#[from] DecodeError),
    #[error("Checksum verification failed. Expected: {0}, Got: {1}")]
    ChecksumVerify(u64, u64),
    #[error("Invalid flatbuffer: {0}")]
    InvalidFlatbuffer(#[from] flatbuffers::InvalidFlatbuffer),
    #[error("TableIndexOffsetEmpty")]
    TableIndexOffsetEmpty,
    #[error("InvalidChecksumLen, Either the data is corrupt or the table Config are incorrectly set ")]
    InvalidChecksumLen,
    #[error("BlockIndexOutOfRange")]
    BlockIndexOutOfRange,
    #[error(transparent)]
    EncryptError(#[from] EncryptError),
    #[error("Compression error: {0}")]
    CompressionError(#[from] CompressError),
    #[error("Iter error: {0}")]
    IterError(#[from] IterError),
    #[error("Tokio JoinError : {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

impl From<MorsTableError> for SSTableError {
    fn from(err: MorsTableError) -> Self {
        SSTableError::new(err)
    }
}
unsafe impl Send for MorsTableError {}
