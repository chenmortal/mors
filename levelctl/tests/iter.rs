use bytesize::ByteSize;
use mors_encrypt::cipher::AesCipher;
use mors_sstable::test_utils::generate_table;
use mors_traits::iter::{generate_kv_slice, CacheIterator, KvCacheIter};
use mors_traits::sstable::CacheTableConcatIter;

#[tokio::test]
async fn test_cache_table_concat_iter() {
    let dir = tempfile::tempdir().unwrap();
    let (tables, range) = generate_table::<AesCipher>(
        dir.into_path(),
        10,
        ByteSize::mib(2).as_u64() as usize,
        "k",
        "v",
    )
    .await;
    let mut iter = CacheTableConcatIter::new(tables, true);
    let kvs = generate_kv_slice(range, "k", "v");
    for (k, v) in kvs {
        match iter.next() {
            Ok(b) => {
                assert!(b);
                assert_eq!(k, iter.key().unwrap());
                assert_eq!(v, iter.value().unwrap());
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
            }
        };
    }
    assert!(!iter.next().unwrap());
}
