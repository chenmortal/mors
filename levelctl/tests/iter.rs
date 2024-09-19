use bytesize::ByteSize;
use mors_common::kv::{Meta, ValueMeta};
use mors_encrypt::cipher::AesCipher;
use mors_sstable::table::Table;
use mors_sstable::test_utils::generate_table;
use mors_traits::iter::{
    generate_kv_slice, CacheIterator, KvCacheIter, KvCacheIterator,
    KvCacheMergeIterator,
};
use mors_traits::sstable::CacheTableConcatIter;
use mors_traits::sstable::TableTrait;

#[tokio::test]
async fn test_cache_table_concat_iter() {
    let dir = tempfile::tempdir().unwrap();
    let (tables, range) = generate_table::<AesCipher>(
        dir.into_path(),
        10,
        ByteSize::mib(2).as_u64() as usize,
        "k",
        "v",
        Meta::default(),
    )
    .await;
    let mut iter = CacheTableConcatIter::new(tables, true);
    let kvs = generate_kv_slice(range, "k", "v", Meta::default());
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
#[tokio::test]
async fn test_cache_merge_iter() {
    let dir = tempfile::tempdir().unwrap();
    let (mut tables, range) = generate_table::<AesCipher>(
        dir.into_path(),
        10,
        ByteSize::mib(2).as_u64() as usize,
        "k",
        "v",
        Meta::default(),
    )
    .await;
    let other = tables.drain(1..).collect::<Vec<_>>();
    let other = CacheTableConcatIter::new(other, true);
    let first = tables.pop().unwrap();
    let mut iter = KvCacheMergeIterator::new(vec![
        Box::new(first.iter(true)),
        Box::new(other),
    ])
    .unwrap();
    let kvs = generate_kv_slice(range, "k", "v", Meta::default());
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
#[tokio::test]
async fn test_other_cache_merge_iter() {
    let dir = tempfile::tempdir().unwrap();
    let (mut tables, range) = generate_table::<AesCipher>(
        dir.into_path(),
        20,
        ByteSize::mib(2).as_u64() as usize,
        "",
        "",
        Meta::default(),
    )
    .await;
    let mut out: Vec<Box<dyn KvCacheIterator<ValueMeta>>> = Vec::new();
    let other = tables.drain(16..).collect::<Vec<_>>();
    let mut first = other[0].iter(true);
    first.next().unwrap();
    let k = first.key().unwrap();
    let k = String::from_utf8(k.key().to_vec())
        .unwrap()
        .trim()
        .parse::<u64>()
        .unwrap()..range.end;

    for t in other.iter().rev() {
        out.push(Box::new(t.iter(true)));
    }
    let valid: Vec<Table<AesCipher>> = Vec::new();
    out.push(Box::new(CacheTableConcatIter::new(valid.clone(), true)));
    let mut iter = KvCacheMergeIterator::new(out).unwrap();

    let kvs = generate_kv_slice(k, "", "", Meta::default());
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
