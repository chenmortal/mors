use std::collections::VecDeque;
use std::sync::Arc;

use crate::file_id::MemtableId;
use crate::{
    kms::Kms,
    kv::{Entry, ValueMeta},
    ts::{KeyTs, TxnTs},
};

pub trait Memtable<K:Kms>: Sized {
    type ErrorType;
    type MemtableBuilder: MemtableBuilder<Self,K>;
    fn get(&self, key_ts: &KeyTs) -> Option<(TxnTs, ValueMeta)>;
    fn push(&mut self, entry: &Entry) -> Result<(), Self::ErrorType>;
    fn size(&self) -> usize;
}
pub trait MemtableBuilder<M: Memtable<K> + Sized,K:Kms>: Default {
   
    fn open(
        &self,
        kms: K,
        id: MemtableId,
    ) -> Result<M, M::ErrorType>;

    fn open_exist(&self,kms: K) -> Result<VecDeque<Arc<M>>, M::ErrorType>;

    fn new(&self,kms: K) -> Result<M,M::ErrorType>;

    fn read_only(&self)->bool;
}
