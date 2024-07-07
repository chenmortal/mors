use crate::default::{WithDir, WithReadOnly};
use crate::file_id::MemtableId;
use crate::{
    kms::Kms,
    kv::{Entry, ValueMeta},
    ts::{KeyTs, TxnTs},
};
use std::collections::VecDeque;
use std::error::Error;
use std::fmt::Display;
use std::sync::Arc;
use thiserror::Error;

pub trait MemtableTrait<K: Kms>: Sized + Send + Sync + 'static {
    type ErrorType: Into<MemtableError>;
    type MemtableBuilder: MemtableBuilderTrait<Self, K>;
    fn get(&self, key_ts: &KeyTs) -> Option<(TxnTs, ValueMeta)>;
    fn push(&mut self, entry: &Entry) -> Result<(), MemtableError>;
    fn size(&self) -> usize;
    fn max_version(&self) -> TxnTs;
}
pub trait MemtableBuilderTrait<M: MemtableTrait<K> + Sized, K: Kms>:
    Default + WithDir + WithReadOnly
{
    fn open(&self, kms: K, id: MemtableId) -> Result<M, MemtableError>;

    fn open_exist(&self, kms: K) -> Result<VecDeque<Arc<M>>, MemtableError>;

    fn build(&self, kms: K) -> Result<M, MemtableError>;
}
#[derive(Error, Debug)]
pub struct MemtableError(Box<dyn Error>);
impl MemtableError {
    pub fn new<E: Error + 'static>(error: E) -> Self {
        MemtableError(Box::new(error))
    }
}
impl Display for MemtableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemtableError Error: {}", self.0)
    }
}
