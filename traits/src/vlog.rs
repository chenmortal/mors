use crate::{
    default::{WithDir, WithReadOnly},
    kms::Kms,
};
use mors_common::kv::{Entry, ValuePointer};
use std::{error::Error, fmt::Display, io, slice::IterMut};
use thiserror::Error;

pub trait VlogCtlTrait<K: Kms>: Sized + Send + Sync + 'static {
    type ErrorType: Into<VlogError>;
    type Discard: DiscardTrait;
    type VlogCtlBuilder: VlogCtlBuilderTrait<Self, K>;
    // fn latest_logfile(&self) -> Result<LogFileWrapper<K>, VlogError>;
    fn writeable_offset(&self) -> usize;
    fn vlog_file_size(&self) -> usize;
    fn write<'a>(
        &self,
        iter_mut: Vec<IterMut<'a, (Entry, ValuePointer)>>,
    ) -> impl std::future::Future<Output = Result<(), VlogError>> + Send;
    const MAX_VLOG_SIZE: usize;
    const MAX_VLOG_FILE_SIZE: usize;
}
pub trait VlogCtlBuilderTrait<V: VlogCtlTrait<K>, K: Kms>:
    WithDir + WithReadOnly + Default
{
    fn build(
        &mut self,
        kms: K,
    ) -> impl std::future::Future<Output = Result<V, VlogError>>;
    fn build_discard(&self) -> Result<V::Discard, VlogError>;
}

pub trait DiscardTrait: Clone + Send + Sync + 'static {
    fn update(&self, fd: u64, discard: i64) -> io::Result<u64>;
}
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
