use bytesize::ByteSize;
use log::{trace, LevelFilter};
use mors_common::closer::Closer;
use mors_common::kv::Meta;
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
use std::fs::create_dir;
use std::path::PathBuf;
use std::time::SystemTime;

pub type TestTable = Table<AesCipher>;
pub type TestLevelCtlBuilder = LevelCtlBuilder<TestTable, MorsKms>;
pub type TestTableBuilder = TableBuilder<AesCipher>;
pub type TestVlogCtlBuilder = VlogCtlBuilder<MorsKms>;

#[test]
fn test_kv_size() {
    let pop = generate_kv_slice(0..1, "k", "v", Meta::default())
        .pop()
        .unwrap();
    let (k, v) = pop;
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
        .set_base_level_total_size(ByteSize::mib(5).as_u64() as usize)
        .set_level_size_multiplier(2)
        .set_table_size_multiplier(2)
        .set_level0_table_size(ByteSize::mib(2).as_u64() as usize)
        .set_level0_num_tables_stall(100)
        .set_level0_tables_len(2);

    let kms = MorsKms::default();
    let level_ctl = builder.build(kms.clone()).await.unwrap();

    let discard = Discard::new(&dir).unwrap();

    let (tables, range) = generate_table(
        dir,
        30,
        ByteSize::mib(2).as_u64() as usize,
        "k",
        "v",
        Meta::default(),
    )
    .await;

    for table in tables {
        let r = level_ctl.push_level0(table).await;
        assert!(r.is_ok());
    }
    let compact_task = Closer::new("levectl compact");
    // tokio::spawn(level_ctl.clone().spawn_compact(compact_task, kms, discard));
    let mut count = 0;
    let mut start = SystemTime::now();
    for (k, v) in generate_kv_slice(range, "k", "v", Meta::default()) {
        let result = level_ctl.get(&k).await;
        assert!(result.is_ok());
        let (_t, value) = result.unwrap().unwrap();
        assert_eq!(v, value.unwrap());
        count += 1;
        if count % 1000 == 0 {
            let duration = start.elapsed().unwrap();
            trace!("count:{} for duration {}", count, duration.as_secs());
            start = SystemTime::now();
        }
        // let (_txn, value) = level_ctl.get(&k).await.unwrap().unwrap();
        // assert_eq!(v, value.unwrap());
    }
}
