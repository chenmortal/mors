use mors_encrypt::registry::Kms;

use crate::{
    kv::{Entry, ValueMeta},
    ts::{KeyTs, TxnTs},
};
use crate::file_id::{FileId, MemtableId};

pub trait Memtable {
    type ErrorType;
    type MemtableBuilder;
    type Id: FileId;

    fn get(&self, key_ts: &KeyTs) -> Option<(TxnTs, ValueMeta)>;
    fn push(&mut self, entry: &Entry) -> Result<(), Self::ErrorType>;
    fn size(&self) -> usize;
    fn new(builder: &Self::MemtableBuilder) -> Self;
    fn open(builder: &Self::MemtableBuilder, kms: Kms, id:MemtableId) -> Self;
}
