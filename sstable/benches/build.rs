#[allow(unused)]
use std::mem::replace;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};

use mors_common::{compress::CompressionType, kv::ValueMeta, ts::KeyTs};
use mors_encrypt::cipher::AesCipher;
use mors_sstable::table::TableBuilder;
use mors_traits::{
    default::WithDir,
    iter::{generate_kv, CacheIterator, KvCacheIter, SeqIter},
    sstable::{SSTableError, TableBuilderTrait, TableTrait},
};
// use mors_sstable::table::Table;
// type TestTable = Table<AesCipher>;
type TestTableBuilder = TableBuilder<AesCipher>;

async fn build_table(
    kv: &Vec<(KeyTs, ValueMeta)>,
    block_size: usize,
    compression: CompressionType,
) -> Result<(), SSTableError> {
    let tempdir = tempfile::tempdir().unwrap();
    let mut builder = TestTableBuilder::default();
    builder.set_block_size(block_size);
    builder.set_compression(compression);
    builder.set_dir(tempdir.path().to_path_buf());
    let iter = SeqIter::new_with_kv(kv);
    let next_id = Arc::new(AtomicU32::new(1));
    let table = builder.build_l0(iter, next_id, None).await?;
    assert!(table.is_some());
    let table = table.unwrap();
    let mut table_iter = table.iter(false);
    let mut iter = SeqIter::new_with_kv(kv);
    while iter.next().unwrap() {
        let n = table_iter.next();
        assert!(n.is_ok());
        assert!(n.unwrap());
        assert_eq!(iter.key(), table_iter.key());
        assert_eq!(iter.value(), table_iter.value());
    }
    tempdir.close().unwrap();
    Ok(())
}
fn bench_build_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_table");
    static KB: usize = 1024;
    // static MB: usize = 1024 * KB;
    let (key, value) = generate_kv(1, "test").pop().unwrap();
    let size = key.len() + value.len();
    for (count, block_size) in [
        (100_000, 16 * KB),
        (100_000, 32 * KB),
        (500_000, 16 * KB),
        (500_000, 32 * KB),
        (1_000_000, 16 * KB),
        (1_000_000, 32 * KB),
    ]
    .iter()
    {
        let kv = generate_kv(*count, "test");

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .unwrap();

        group.throughput(Throughput::Bytes((*count * size as u32) as u64));
        group.sample_size(10);
        let par = "count:".to_string()
            + (count).to_string().as_str()
            + "-data:"
            + ((*count as usize * size) / KB).to_string().as_str()
            + "KB"
            + "-block_size:"
            + (block_size / KB).to_string().as_str()
            + "KB";
        group.bench_with_input(
            BenchmarkId::new("no_compression", par.clone()),
            &kv,
            |b, data| {
                b.to_async(&runtime).iter(|| {
                    build_table(data, *block_size, CompressionType::None)
                })
            },
        );
    }
    group.finish();
}
criterion_group!(benches, bench_build_table);
criterion_main!(benches);
