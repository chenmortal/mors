use std::fs::create_dir;
use std::path::PathBuf;

use bytesize::ByteSize;
use log::LevelFilter;
use mors_common::closer::Closer;
use mors_encrypt::cipher::AesCipher;
use mors_encrypt::registry::MorsKms;
use mors_levelctl::ctl::LevelCtlBuilder;
use mors_sstable::table::{Table, TableBuilder};
use mors_sstable::test_utils::generate_table;
use mors_traits::default::WithDir;
use mors_traits::iter::generate_kv_slice;
use mors_traits::levelctl::LevelCtlBuilderTrait;
use mors_traits::levelctl::LevelCtlTrait;
use mors_vlog::discard::Discard;
use mors_vlog::vlogctl::VlogCtlBuilder;

pub type TestTable = Table<AesCipher>;
pub type TestLevelCtlBuilder = LevelCtlBuilder<TestTable, MorsKms>;
pub type TestTableBuilder = TableBuilder<AesCipher>;
pub type TestVlogCtlBuilder = VlogCtlBuilder<MorsKms>;

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
        .set_num_compactors(1)
        .set_levelmax2max_compaction(true)
        .set_base_level_total_size(ByteSize::mib(10).as_u64() as usize)
        .set_level_size_multiplier(2)
        .set_table_size_multiplier(2)
        .set_level0_table_size(ByteSize::mib(5).as_u64() as usize)
        .set_level0_tables_len(3);
    // builder.set_cache();
    let kms = MorsKms::default();
    let level_ctl = builder.build(kms.clone()).await.unwrap();

    let discard = Discard::new(&dir).unwrap();

    let (tables, _) =
        generate_table(dir, 10, ByteSize::mib(2).as_u64() as usize, "k", "v")
            .await;

    for table in tables {
        let r = level_ctl.push_level0(table).await;
        assert!(r.is_ok());
    }
    let compact_task = Closer::new("levectl compact");
    level_ctl.spawn_compact(compact_task, kms, discard).await;
}
