use crate::{
    default::{WithDir, WithReadOnly},
    kms::Kms,
};
use std::{error::Error, fmt::Display};
use thiserror::Error;

pub trait VlogCtlTrait<K: Kms, D: DiscardTrait>:
    Sized + Send + Sync + 'static
{
    type ErrorType: Into<VlogError>;
    type LevelCtlBuilder: VlogCtlBuilderTrait<Self, K, D>;
}
pub trait VlogCtlBuilderTrait<V: VlogCtlTrait<K, D>, K: Kms, D: DiscardTrait>:
    WithDir + WithReadOnly
{
    fn build(
        &self,
        kms: K,
    ) -> impl std::future::Future<Output = Result<V, VlogError>>;
}

pub trait DiscardTrait {}
#[derive(Error, Debug)]
pub struct VlogError(Box<dyn Error>);
impl VlogError {
    pub fn new<E: Error + 'static>(error: E) -> Self {
        VlogError(Box::new(error))
    }
}
impl Display for VlogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VlogCtlError: {}", self.0)
    }
}
unsafe impl Send for VlogError {}
