use mors_common::ts::TxnTs;
use std::{error::Error, fmt::Display};
use thiserror::Error;

pub trait TxnManagerTrait: Sized {
    type ErrorType: Into<TxnManagerError>;
    type TxnManagerBuilder: TxnManagerBuilderTrait<Self>;
}
pub trait TxnManagerBuilderTrait<T: TxnManagerTrait>: Default {
    fn build(
        &self,
        max_version: TxnTs,
    ) -> impl std::future::Future<Output = Result<T, TxnManagerError>>;
}
#[derive(Error, Debug)]
pub struct TxnManagerError(Box<dyn Error>);
impl TxnManagerError {
    pub fn new<E: Error + 'static>(error: E) -> Self {
        TxnManagerError(Box::new(error))
    }
}
impl Display for TxnManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TxnManagerError: {}", self.0)
    }
}
