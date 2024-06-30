use mors_traits::file_id::SSTableId;
use thiserror::Error;
use super::MAGIC_VERSION;
#[derive(Error, Debug)]
pub enum ManifestError {
    #[error("Bad Magic")]
    BadMagic,
    #[error("Bad External Magic Version: {0} is not supported. Expected: {MAGIC_VERSION}")]
    BadExternalMagicVersion(u16, u16),
    #[error("Bad Version: {0} is not supported. Expected: {MAGIC_VERSION}")]
    BadVersion(u16),
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Corrupted Manifest at offset {0} with length {1}")]
    Corrupted(u64, usize),
    #[error("CheckSum Mismatch")]
    CheckSumMismatch,
    #[error("Decode Error: {0}")]
    DecodeError(#[from] prost::DecodeError),
    #[error("MANIFEST invalid, table {0} exists")]
    CreateError(SSTableId),
    #[error("MANIFEST removes non-existing table {0}")]
    DeleteError(SSTableId),
    #[error("No Manifest Found,no write operation is allowed.")]
    NoManifest,
    #[error("Table {0} not found")]
    TableNotFound(SSTableId),
}