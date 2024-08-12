use log::LevelFilter;
use mors_common::{
    kv::{Meta, ValueMeta},
    ts::{KeyTs, KeyTsBorrow},
};
use mors_encrypt::cipher::AesCipher;
use mors_sstable::table::{Table, TableBuilder};
use mors_traits::{default::WithDir, sstable::TableBuilderTrait};
use mors_traits::{
    iter::{CacheIter, CacheIterator, IterError, KvCacheIter},
    sstable::SSTableError,
};
use std::{
    fs::create_dir,
    path::PathBuf,
    result::Result,
    sync::{atomic::AtomicU32, Arc},
};
pub(crate) struct SeqIter {
    index: Option<usize>,
    kv: Vec<(KeyTs, ValueMeta)>,
    k: Option<Vec<u8>>,
    v: Option<ValueMeta>,
}
type TestTable = Table<AesCipher>;
type TestTableBuilder = TableBuilder<AesCipher>;
#[tokio::test]
async fn test_build_l0() -> Result<(), SSTableError> {
    let mut logger = env_logger::builder();
    logger.filter_level(LevelFilter::Trace);
    logger.init();
    let mut builder = TestTableBuilder::default();
    builder.set_block_size(4 * 1024);
    let test_dir = "./tests/test_data/";
    let dir = PathBuf::from(test_dir);
    if !dir.exists() {
        create_dir(&dir).unwrap();
    }
    builder.set_dir(PathBuf::from(test_dir));
    let iter = SeqIter::new(10000, "k");
    let next_id = Arc::new(AtomicU32::new(1));
    let table = builder.build_l0(iter, next_id, None).await?;
    Ok(())
}
impl SeqIter {
    pub fn new(count: u32, prefix: &str) -> Self {
        let kv = generate_kv(count, prefix);
        Self {
            index: None,
            kv,
            k: None,
            v: None,
        }
    }
}
impl CacheIter for SeqIter {
    type Item = usize;

    fn item(&self) -> Option<&Self::Item> {
        self.index.as_ref()
    }
}
impl CacheIterator for SeqIter {
    fn next(&mut self) -> Result<bool, IterError> {
        match self.index.as_mut() {
            Some(index) => {
                if *index >= self.kv.len() - 1 {
                    Ok(false)
                } else {
                    *index += 1;
                    let (k, v) = self.kv[*index].clone();
                    self.k = k.encode().into();
                    self.v = v.into();
                    Ok(true)
                }
            }
            None => {
                self.index = Some(0);
                let (k, v) = self.kv[0].clone();
                self.k = k.encode().into();
                self.v = v.into();
                Ok(true)
            }
        }
    }
}
impl KvCacheIter<ValueMeta> for SeqIter {
    fn key(&self) -> Option<KeyTsBorrow<'_>> {
        if let Some(k) = self.k.as_ref() {
            return Some(k.as_slice().into());
        }
        None
    }

    fn value(&self) -> Option<ValueMeta> {
        self.v.clone()
    }
}

fn generate_kv(count: u32, prefix: &str) -> Vec<(KeyTs, ValueMeta)> {
    let mut kv = Vec::with_capacity(count as usize);
    for i in 0..count {
        let k = prefix.to_string() + &format!("{:04}", i);
        let key = KeyTs::new(k.into(), 0.into());
        let v = format!("{}", i);
        let mut value = ValueMeta::default();
        value.set_value(v.into());
        value.set_meta(Meta::from_bits(b'A').unwrap());
        value.set_user_meta(0);
        kv.push((key, value));
    }
    kv
}

