use crate::{
    default::{WithDir, WithReadOnly},
    kms::Kms,
};
use std::{error::Error, fmt::Display};
use thiserror::Error;

pub trait VlogCtlTrait<K: Kms>: Sized + Send + Sync + 'static {
    type ErrorType: Into<VlogError>;
    type Discard: DiscardTrait;
    type VlogCtlBuilder: VlogCtlBuilderTrait<Self, K>;
}
pub trait VlogCtlBuilderTrait<V: VlogCtlTrait<K>, K: Kms>:
    WithDir + WithReadOnly
{
    fn build(
        &self,
        kms: K,
    ) -> impl std::future::Future<Output = Result<V, VlogError>>;
    fn build_discard(&self) -> Result<V::Discard, VlogError>;
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
