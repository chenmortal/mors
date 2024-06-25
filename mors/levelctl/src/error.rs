use thiserror::Error;

use crate::manifest::error::ManifestError;


#[derive(Error, Debug)]
pub enum MorsLevelCtlError {
    #[error("IO Error: {0}")]
    IOErr(#[from] std::io::Error),
    #[error("Manifest Error: {0}")]
    ManifestErr(#[from] ManifestError),

}
