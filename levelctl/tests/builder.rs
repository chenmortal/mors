use std::fs::create_dir;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use bytesize::ByteSize;
use log::LevelFilter;
use mors_common::closer::Closer;
use mors_encrypt::cipher::AesCipher;
use mors_encrypt::registry::MorsKms;
use mors_levelctl::ctl::LevelCtlBuilder;
use mors_sstable::table::{Table, TableBuilder};
use mors_traits::default::WithDir;
use mors_traits::iter::{generate_kv_slice, SeqIter};
use mors_traits::levelctl::LevelCtlBuilderTrait;
use mors_traits::levelctl::LevelCtlTrait;
use mors_traits::sstable::TableBuilderTrait;
use mors_vlog::discard::Discard;
use mors_vlog::vlogctl::VlogCtlBuilder;

type TestTable = Table<AesCipher>;
type TestLevelCtlBuilder = LevelCtlBuilder<TestTable, MorsKms>;
type TestTableBuilder = TableBuilder<AesCipher>;
type TestVlogCtlBuilder = VlogCtlBuilder<MorsKms>;
async fn generate_table(
    dir: PathBuf,
    count: u64,
    table_size: usize,
) -> Vec<TestTable> {
    let mut builder = TestTableBuilder::default();

    builder.set_block_size(ByteSize::kib(8).as_u64() as usize);
    builder.set_dir(dir);
    let next_id = Arc::new(AtomicU32::new(1));

    let (k, v) = generate_kv_slice(0..1, "k", "v").pop().unwrap();
    let kv_count = (table_size / (k.len() + v.len())) as u64;

    let mut task = Vec::with_capacity(count as usize);
    for i in 0..count {
        let builder_c = builder.clone();
        let next_id_c = next_id.clone();
        task.push(tokio::spawn(async move {
            let start = i * kv_count;
            let end = (i + 1) * kv_count;
            let kv = generate_kv_slice(start..end, "k", "v");
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
    tables
}
#[test]
fn test_kv_size() {
    let (k, v) = generate_kv_slice(0..1, "k", "v").pop().unwrap();
    dbg!(k.len());
    dbg!(v.len());
}
#[tokio::test]
async fn test_builder() {
    let mut logger = env_logger::builder();
    logger.filter_level(LevelFilter::Trace);
    logger.init();
    let mut builder = TestLevelCtlBuilder::default();
    let dir = "/tmp/levelctl";
    let dir = PathBuf::from(dir);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).unwrap();
    }
    create_dir(&dir).unwrap();
    builder
        .set_dir(dir.clone())
        .set_max_level(4u32.into())
        .set_num_compactors(3)
        .set_levelmax2max_compaction(true)
        .set_base_level_size(ByteSize::mib(10).as_u64() as usize)
        .set_level_size_multiplier(2)
        .set_table_size_multiplier(2)
        .set_level0_size(ByteSize::mib(5).as_u64() as usize)
        .set_level0_tables_len(3);
    let kms = MorsKms::default();
    let level_ctl = builder.build(kms.clone()).await.unwrap();

    let discard = Discard::new(&dir).unwrap();

    let tables =
        generate_table(dir, 10, ByteSize::mib(2).as_u64() as usize).await;

    for table in tables {
        let r = level_ctl.push_level0(table).await;
        assert!(r.is_ok());
    }
    let compact_task = Closer::new("levectl compact");
    level_ctl.spawn_compact(compact_task, kms, discard).await;
}
