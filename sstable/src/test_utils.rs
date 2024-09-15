use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use bytesize::ByteSize;
use mors_common::kv::Meta;
use mors_traits::cache::CacheBuilder;
use mors_traits::default::WithDir;
use mors_traits::iter::{generate_kv_slice, SeqIter};
use mors_traits::kms::KmsCipher;
use mors_traits::sstable::TableBuilderTrait;

use crate::cache::MorsCacheBuilder;
use crate::table::{Table, TableBuilder};

pub async fn generate_table<K: KmsCipher>(
    dir: PathBuf,
    count: u64,
    table_size: usize,
    k_prefix: &'static str,
    v_prefix: &'static str,
    meta: Meta,
) -> (Vec<Table<K>>, Range<u64>) {
    let mut builder = TableBuilder::default();

    builder.set_block_size(ByteSize::kib(8).as_u64() as usize);
    builder.set_dir(dir);
    let cache_builder = MorsCacheBuilder::default();
    let cache = cache_builder.build().unwrap();
    builder.set_cache(cache);
    let next_id = Arc::new(AtomicU32::new(1));

    let (k, v) = generate_kv_slice(0..1, k_prefix, v_prefix, meta)
        .pop()
        .unwrap();
    let kv_count = (table_size / (k.len() + v.len())) as u64;

    let mut task = Vec::with_capacity(count as usize);

    for i in 0..count {
        let builder_c = builder.clone();
        let next_id_c = next_id.clone();
        task.push(tokio::spawn(async move {
            let start = i * kv_count;
            let end = (i + 1) * kv_count;
            let kv = generate_kv_slice(start..end, k_prefix, v_prefix, meta);
            let iter = SeqIter::new_with_kv(&kv);
            let table = builder_c.build_l0(iter, next_id_c, None).await;
            assert!(table.is_ok());
            let table = table.unwrap();
            assert!(table.is_some());
            table.unwrap()
        }));
    }
    let mut tables = Vec::with_capacity(count as usize);
    for ele in task {
        tables.push(ele.await.unwrap());
    }
    (tables, 0..count * kv_count)
}
