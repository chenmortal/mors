use std::sync::PoisonError;

use mors_common::{file_id::SSTableId, ts::KeyTs};
use mors_traits::{
    kms::KmsError,
    levelctl::{Level, LevelCtlError},
    sstable::SSTableError,
};
use thiserror::Error;

use crate::manifest::error::ManifestError;
#[derive(Error, Debug)]
pub enum MorsLevelCtlError {
    #[error("IO Error: {0}")]
    IOErr(#[from] std::io::Error),
    #[error("Manifest Error: {0}")]
    ManifestErr(#[from] ManifestError),
    #[error("Acquire Error: {0}")]
    AcquireError(#[from] tokio::sync::AcquireError),
    #[error("Kms Error: {0}")]
    KmsError(#[from] KmsError),
    #[error("SSTable Error: {0}")]
    SSTableError(#[from] SSTableError),
    #[error("Join Error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Level Handler Error: {0}")]
    LevelHandlerError(#[from] LevelHandlerError),
    #[error("Poison error: {0}")]
    PoisonError(String),
    #[error("Fill Tables Error")]
    FillTablesError,
    #[error("Empty Compact Target")]
    EmptyCompactTarget,
}

impl<T> From<PoisonError<T>> for MorsLevelCtlError {
    fn from(e: PoisonError<T>) -> MorsLevelCtlError {
        MorsLevelCtlError::PoisonError(e.to_string())
    }
}
#[derive(Error, Debug)]
pub enum LevelHandlerError {
    #[error("SSTable Overlap Error:Level {0:?} Pre SSTable {1:?} biggest {2:?} > This SSTable {3:?} smallest {4:?}")]
    TableOverlapError(Level, SSTableId, KeyTs, SSTableId, KeyTs),
    #[error("SSTable Inner Sort Error:Level {0:?} SSTable {1:?} smallest KeyTs {2:?} >= biggest {3:?}")]
    TableInnerSortError(Level, SSTableId, KeyTs, KeyTs),
}
unsafe impl Send for MorsLevelCtlError {}
impl From<MorsLevelCtlError> for LevelCtlError {
    fn from(e: MorsLevelCtlError) -> Self {
        LevelCtlError::new(e)
    }
}
