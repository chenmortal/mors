use mors_common::{
    compress::CompressionType,
    kv::{Meta, ValueMeta},
    ts::{KeyTs, KeyTsBorrow},
};
use mors_encrypt::cipher::AesCipher;
use mors_sstable::table::{Table, TableBuilder};
use mors_traits::sstable::TableTrait;
use mors_traits::{default::WithDir, sstable::TableBuilderTrait};
use mors_traits::{
    iter::{CacheIter, CacheIterator, IterError, KvCacheIter},
    sstable::SSTableError,
};
use proptest::prelude::ProptestConfig;
use proptest::proptest;
use std::{
    path::Path,
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
async fn build_table(
    dir: &Path,
    count: u32,
    prefix: &str,
    compression: CompressionType,
) -> Result<TestTable, SSTableError> {
    let mut builder = TestTableBuilder::default();
    builder.set_block_size(4 * 1024);
    builder.set_compression(compression);
    builder.set_dir(dir.to_path_buf());
    let iter = SeqIter::new(count, prefix);
    let next_id = Arc::new(AtomicU32::new(1));
    let table = builder.build_l0(iter, next_id, None).await;
    if table.is_err() {
        dbg!(count);
    }
    Ok(table?.unwrap())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(99))]
    #[test]
    fn test_table_iter(count in 1..1000u32) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(test_iter(count,CompressionType::None));
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(99))]
    #[test]
    fn test_table_zstd(count in 1..1000u32) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(test_iter(count,CompressionType::ZSTD(5)));
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(99))]
    #[test]
    fn test_table_snappy(count in 1..1000u32) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(test_iter(count,CompressionType::Snappy));
    }
}
async fn test_iter(count: u32, compression: CompressionType) {
    tempfile::tempfile().unwrap();
    let tempdir = tempfile::tempdir().unwrap();
    let table = build_table(tempdir.path(), count, "key", compression)
        .await
        .unwrap();
    let mut table_iter = table.iter(false);
    let mut iter = SeqIter::new(count, "key");
    while iter.next().unwrap() {
        let n = table_iter.next();
        assert!(n.is_ok());
        assert!(n.unwrap());
        assert_eq!(iter.key(), table_iter.key());
        assert_eq!(iter.value(), table_iter.value());
    }
    tempdir.close().unwrap();
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
        let k = prefix.to_string() + &format!("{:06}", i);
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
