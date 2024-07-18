use mors_traits::vlog::VlogError;
use thiserror::Error;
#[derive(Error, Debug)]
pub enum MorsVlogError {}
impl From<MorsVlogError> for VlogError {
    fn from(e: MorsVlogError) -> VlogError {
        VlogError::new(e)
    }
}
