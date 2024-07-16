use crate::default::{WithDir, WithReadOnly};
use crate::kms::Kms;
use crate::skip_list::SkipListTrait;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt::Display;
use std::sync::Arc;
use mors_common::file_id::MemtableId;
use mors_common::kv::{Entry, ValueMeta};
use mors_common::ts::{KeyTs, TxnTs};
use thiserror::Error;

pub trait MemtableTrait<T: SkipListTrait, K: Kms>:
    Sized + Send + Sync + 'static
{
    type ErrorType: Into<MemtableError>;
    type MemtableBuilder: MemtableBuilderTrait<Self, T, K>;
    fn get(
        &self,
        key_ts: &KeyTs,
    ) -> Result<Option<(TxnTs, ValueMeta)>, MemtableError>;
    fn push(&mut self, entry: &Entry) -> Result<(), MemtableError>;
    fn size(&self) -> usize;
    fn is_full(&self) -> bool;
    fn id(&self) -> MemtableId;
    fn max_version(&self) -> TxnTs;
    fn skip_list(&self) -> T;
    fn flush(&mut self) -> Result<(), MemtableError>;
}
pub trait MemtableBuilderTrait<
    M: MemtableTrait<T, K> + Sized,
    T: SkipListTrait,
    K: Kms,
>: Default + WithDir + WithReadOnly + Clone + Send + Sync
{
    fn open(&self, kms: K, id: MemtableId) -> Result<M, MemtableError>;

    fn open_exist(&self, kms: K) -> Result<VecDeque<Arc<M>>, MemtableError>;

    fn build(&self, kms: K) -> Result<M, MemtableError>;
    fn set_num_memtables(&mut self, num_memtables: usize);
    fn set_memtable_size(&mut self, memtable_size: usize);
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
