use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use mors_common::kv::ValueMeta;
use mors_common::ts::{KeyTs, KeyTsBorrow};
use mors_encrypt::cipher::AesCipher;
use mors_sstable::table::TableBuilder;
use mors_traits::default::WithDir;
use mors_traits::iter::{
    CacheIter, CacheIterator, IterError, KvCacheIter, KvCacheIterator,
    KvSeekIter,
};
use mors_traits::sstable::TableBuilderTrait;
use rand::Rng;
use rand::{rngs::StdRng, SeedableRng};
use sha2::Digest;
use sha2::Sha256;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let mut builder = TableBuilder::default();
    let path = PathBuf::from("/tmp/sstable");
    dbg!(path.exists());
    if path.exists() {
        println!("remove dir {:?}", path);
        std::fs::remove_dir_all(&path).unwrap();
    }
    create_dir_all(path.clone()).unwrap();
    builder.set_dir(path);
    let rng = RngIter::new(get_rng(), 10000);
    let next_id = Arc::new(AtomicU32::new(0));
    let _table = builder
        .build_l0(rng, next_id, None::<AesCipher>)
        .await
        .unwrap();
}

fn get_rng() -> StdRng {
    let seed = "abc";
    let mut hasher = Sha256::new();
    hasher.update(seed);
    let result = hasher.finalize();
    let seed = result.into();
    StdRng::from_seed(seed)
}

#[test]
fn test_rang() {
    let mut rng = get_rng();
    let mut rng_clone = get_rng();
    for _ in 0..10 {
        let rand: u64 = rng.gen();
        let rang_clone: u64 = rng_clone.gen();
        assert_eq!(rand, rang_clone);
    }
}
#[test]
fn test_rng_iter() {
    let mut iter = RngIter::new(get_rng(), 10);
    let mut iter_clone = RngIter::new(get_rng(), 10);
    while iter.next().unwrap() {
        assert!(iter_clone.next().unwrap());
        assert_eq!(iter.key(), iter_clone.key());
        assert_eq!(iter.value(), iter_clone.value());
    }
}
struct RngIter {
    rng: StdRng,
    items: usize,
    count: usize,
    key: Option<Vec<u8>>,
    value: Option<ValueMeta>,
}
impl RngIter {
    fn new(rng: StdRng, items: usize) -> Self {
        Self {
            rng,
            items,
            count: 0,
            key: None,
            value: None,
        }
    }
}
impl CacheIter for RngIter {
    type Item = Vec<u8>;

    fn item(&self) -> Option<&Self::Item> {
        self.key.as_ref()
    }
}
impl CacheIterator for RngIter {
    fn next(&mut self) -> Result<bool, IterError> {
        if self.count < self.items {
            let key: [u8; 16] = self.rng.gen();
            let txn_ts: u64 = self.rng.gen();
            let key_ts =
                KeyTs::new(key.to_vec().into(), txn_ts.into()).encode();
            self.key = Some(key_ts);
            let mut value_meta = ValueMeta::default();
            let value: [u8; 16] = self.rng.gen();
            value_meta.set_value(value.to_vec().into());
            self.value = Some(value_meta);
            self.count += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl KvCacheIter<ValueMeta> for RngIter {
    fn key(&self) -> Option<KeyTsBorrow<'_>> {
        if let Some(s) = self.item() {
            return Some(s.as_slice().into());
        }
        None
    }

    fn value(&self) -> Option<ValueMeta> {
        self.value.clone()
    }
}
impl KvSeekIter for RngIter {
    fn seek(&mut self, k: KeyTsBorrow<'_>) -> Result<bool, IterError> {
        let key = k.as_ref().to_vec();
        let txn_ts = k.txn_ts().to_u64();
        let key_ts = KeyTs::new(key.into(), txn_ts.into()).encode();
        self.key = Some(key_ts);
        Ok(true)
    }
}

impl KvCacheIterator<ValueMeta> for RngIter {}
