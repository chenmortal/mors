use std::sync::Arc;
use std::time::SystemTime;

use bytesize::ByteSize;
use log::error;
use log::info;
use log::LevelFilter;
use mors_common::test::{gen_random_entries, get_rng};
use mors_encrypt::registry::{MorsKms, MorsKmsBuilder};
use mors_memtable::memtable::{Memtable, MemtableBuilder};
use mors_skip_list::skip_list::SkipList;
use mors_traits::default::WithDir;
use mors_traits::kms::KmsBuilder;
use mors_traits::memtable::MemtableBuilderConfig;
use mors_traits::memtable::MemtableBuilderTrait;
use mors_traits::memtable::MemtableTrait;
use mors_wal::storage::mmap::MmapFile;
type TestMemtableBuilder = MemtableBuilder<SkipList>;
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_read() {
    let mut logger = env_logger::builder();
    logger.filter_level(LevelFilter::Trace);
    logger.init();

    let tempdir = tempfile::tempdir().unwrap();
    let mut kms_builder = MorsKmsBuilder::default();
    kms_builder.set_dir(tempdir.path().to_path_buf());
    let kms = kms_builder.build().unwrap();

    let mut builder = TestMemtableBuilder::default();
    builder.set_dir(tempdir.path().to_path_buf());
    builder.set_memtable_size(ByteSize::mib(512).as_u64() as usize);
    let memtable_init: Arc<Memtable<SkipList, MorsKms, MmapFile>> =
        Arc::new(builder.build(kms.clone()).unwrap());
    let seeds = vec!["a", "b", "c", "d","e","f","g","h"];
    let start_all = SystemTime::now();
    let mut handlers = Vec::with_capacity(seeds.len());
    for seed in seeds {
        let mut rng = get_rng(seed);
        let memtable = memtable_init.clone();
        handlers.push(tokio::spawn(async move {
            info!("{} Starting write", seed);

            let count = 100_000;
            let mut push_count = 0;
            let mut push_not_count = 0;
            let random = gen_random_entries(&mut rng, count, 1000.into());
            let start = SystemTime::now();
            for entry in &random {
                match memtable.push(entry) {
                    Ok(_) => {
                        push_count += 1;
                        if push_count % 10000 == 0 {
                            info!("{} push_count: {}", seed, push_count);
                        }
                    }
                    Err(e) => {
                        push_not_count += 1;
                        if push_not_count % 10000 == 0 {
                            info!(
                                "{} push_not_count: {}",
                                seed, push_not_count
                            );
                        }
                        error!("{} push error: {:?}", seed, e);
                    }
                };
            }
            info!(
                "{} push_count: {}, push_not_count: {}",
                seed, push_count, push_not_count
            );
            let mut get_count = 0;
            let mut get_error_count = 0;
            let mut get_not_count = 0;
            for entry in random {
                match memtable.get(entry.key_ts()) {
                    Ok(k) => match k {
                        Some((txn, value)) => {
                            get_count += 1;
                            if get_count % 10000 == 0 {
                                info!("{} get_count: {}", seed, get_count);
                            }
                            assert_eq!(txn, entry.version());
                            assert_eq!(&value.unwrap(), entry.value_meta());
                        }
                        None => {
                            get_not_count += 1;
                            if get_not_count % 10000 == 0 {
                                info!(
                                    "{} get_not_count: {}",
                                    seed, get_not_count
                                );
                            }
                        }
                    },
                    Err(e) => {
                        get_error_count += 1;
                        if get_error_count % 10000 == 0 {
                            info!(
                                "{} get_error_count: {}",
                                seed, get_error_count
                            );
                        }
                        error!("{} get error: {:?}", seed, e);
                    }
                };
            }
            let elapsed = start.elapsed().unwrap();
            info!(
                "{} get_count: {}, get_not_count: {}, get_error_count: {}, elapsed: {:?}",
                seed, get_count, get_not_count, get_error_count, elapsed
            );
        }));
    }
    for handler in handlers {
        handler.await.unwrap();
    }

    let elapsed_all = start_all.elapsed().unwrap();
    info!("elapsed_all: {:?}", elapsed_all);
}
