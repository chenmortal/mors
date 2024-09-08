use mors_common::compress::CompressionType;
use mors_common::kv::ValueMeta;
use mors_common::ts::KeyTs;
use mors_encrypt::cipher::AesCipher;
use mors_sstable::table::{Table, TableBuilder};
use mors_traits::iter::generate_kv;
use mors_traits::{default::WithDir, sstable::TableBuilderTrait};
use mors_traits::{iter::SeqIter, sstable::TableTrait};
use mors_traits::{
    iter::{CacheIterator, KvCacheIter},
    sstable::SSTableError,
};
use proptest::prelude::ProptestConfig;
use proptest::proptest;
use std::{
    path::Path,
    result::Result,
    sync::{atomic::AtomicU32, Arc},
};

type TestTable = Table<AesCipher>;
type TestTableBuilder = TableBuilder<AesCipher>;
async fn build_table(
    dir: &Path,
    kv: &Vec<(KeyTs, ValueMeta)>,
    compression: CompressionType,
) -> Result<TestTable, SSTableError> {
    let mut builder = TestTableBuilder::default();
    builder.set_block_size(4 * 1024);
    builder.set_compression(compression);
    builder.set_dir(dir.to_path_buf());
    let iter = SeqIter::new_with_kv(kv);
    let next_id = Arc::new(AtomicU32::new(1));
    let table = builder.build_l0(iter, next_id, None).await;
    assert!(table.is_ok());
    let table = table.unwrap();
    assert!(table.is_some());
    Ok(table.unwrap())
}
#[test]
fn test_build() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(test_iter(100000, CompressionType::None));
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
    let tempdir = tempfile::tempdir().unwrap();
    let kv = generate_kv(count, "key");
    let table = build_table(tempdir.path(), &kv, compression).await.unwrap();
    let mut table_iter = table.iter(false);
    let mut iter = SeqIter::new_with_kv(&kv);
    while iter.next().unwrap() {
        let n = table_iter.next();
        assert!(n.is_ok());
        assert!(n.unwrap());
        assert_eq!(iter.key(), table_iter.key());
        assert_eq!(iter.value(), table_iter.value());
    }
    tempdir.close().unwrap();
}
