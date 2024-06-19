use crate::{kv::{Entry, ValueMeta}, ts::{KeyTs, TxnTs}};

pub trait Memtable {
    type ErrorType;
    fn get(&self, key_ts: &KeyTs) -> Option<(TxnTs,ValueMeta)>;
    fn push(&mut self, entry: &Entry) -> Result<(),Self::ErrorType>;
    fn size(&self) -> usize;
}
