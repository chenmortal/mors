use std::collections::VecDeque;
use std::sync::Arc;

use crate::file_id::MemtableId;
use crate::{
    kms::Kms,
    kv::{Entry, ValueMeta},
    ts::{KeyTs, TxnTs},
};

pub trait MemtableTrait<K: Kms>: Sized {
    type ErrorType;
    type MemtableBuilder: MemtableBuilderTrait<Self, K>;
    fn get(&self, key_ts: &KeyTs) -> Option<(TxnTs, ValueMeta)>;
    fn push(&mut self, entry: &Entry) -> Result<(), Self::ErrorType>;
    fn size(&self) -> usize;
    fn max_version(&self) -> TxnTs;
}
pub trait MemtableBuilderTrait<M: MemtableTrait<K> + Sized, K: Kms>: Default {
    fn open(&self, kms: K, id: MemtableId) -> Result<M, M::ErrorType>;

    fn open_exist(&self, kms: K) -> Result<VecDeque<Arc<M>>, M::ErrorType>;

    fn build(&self, kms: K) -> Result<M, M::ErrorType>;

    fn read_only(&self) -> bool;
}
