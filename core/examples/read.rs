#[allow(unused_imports)]
use log::LevelFilter;
use std::path::PathBuf;
#[cfg(feature = "sync")]
use std::thread;
use std::time::Instant;

use morsdb::Mors;
use morsdb::MorsBuilder;
use morsdb::Result;
const ITERATIONS: usize = 2;
const ELEMENTS: usize = 1_000_000;
// const ELEMENTS: usize = 100_000;
const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 150;
const RNG_SEED: u64 = 3;

fn fill_slice(slice: &mut [u8], rng: &mut fastrand::Rng) {
    let mut i = 0;
    while i + size_of::<u128>() < slice.len() {
        let tmp = rng.u128(..);
        slice[i..(i + size_of::<u128>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u128>()
    }
    if i + size_of::<u64>() < slice.len() {
        let tmp = rng.u64(..);
        slice[i..(i + size_of::<u64>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u64>()
    }
    if i + size_of::<u32>() < slice.len() {
        let tmp = rng.u32(..);
        slice[i..(i + size_of::<u32>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u32>()
    }
    if i + size_of::<u16>() < slice.len() {
        let tmp = rng.u16(..);
        slice[i..(i + size_of::<u16>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u16>()
    }
    if i + size_of::<u8>() < slice.len() {
        slice[i] = rng.u8(..);
    }
}

/// Returns pairs of key, value
fn gen_pair(rng: &mut fastrand::Rng) -> ([u8; KEY_SIZE], Vec<u8>) {
    let mut key = [0u8; KEY_SIZE];
    fill_slice(&mut key, rng);
    let mut value = vec![0u8; VALUE_SIZE];
    fill_slice(&mut value, rng);

    (key, value)
}

fn make_rng() -> fastrand::Rng {
    fastrand::Rng::with_seed(RNG_SEED)
}

fn make_rng_shards(shards: usize, elements: usize) -> Vec<fastrand::Rng> {
    let mut rngs = vec![];
    let elements_per_shard = elements / shards;
    for i in 0..shards {
        let mut rng = make_rng();
        for _ in 0..(i * elements_per_shard) {
            gen_pair(&mut rng);
        }
        rngs.push(rng);
    }

    rngs
}
#[cfg(feature = "sync")]
fn main() {
    // let mut logger = env_logger::builder();
    // logger.filter_level(LevelFilter::Trace);
    // logger.init();
    if let Err(e) = main_impl() {
        eprintln!("Error: {:?}", e.to_string());
    };
}
#[cfg(feature = "sync")]
fn main_impl() -> Result<()> {
    use std::fs::{create_dir_all, remove_dir_all};

    let path = "/tmp/mors_read/data/";
    let dir = PathBuf::from(path);
    if dir.exists() {
        let _ = remove_dir_all(dir.clone());
    }
    create_dir_all(&dir).unwrap();
    let mut builder = MorsBuilder::default();
    builder.set_memtable_size(128 * 1024 * 1024);
    builder.set_dir(dir).set_read_only(false);

    let mors = builder.build()?;
    benchmark(mors);
    Ok(())
}
#[cfg(not(feature = "sync"))]
#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    // let mut logger = env_logger::builder();
    // logger.filter_level(LevelFilter::Trace);
    // logger.init();

    if let Err(e) = main_impl().await {
        eprintln!("Error: {:?}", e.to_string());
    }
}
#[cfg(not(feature = "sync"))]
async fn main_impl() -> Result<()> {
    use std::fs::{create_dir_all, remove_dir_all};

    let path = "/tmp/mors_read/data/";
    let dir = PathBuf::from(path);
    if dir.exists() {
        let _ = remove_dir_all(dir.clone());
    }
    create_dir_all(&dir).unwrap();
    let mut builder = MorsBuilder::default();
    builder.set_memtable_size(128 * 1024 * 1024);
    builder.set_dir(dir).set_read_only(false);

    let mors = builder.build().await?;
    benchmark(mors).await;
    // let v = write_tran.get("key".into()).await?;
    Ok(())
}
#[cfg(feature = "sync")]
fn benchmark(db: Mors) {
    let mut rng = make_rng();

    let start = Instant::now();
    let mut txn = db.begin_write().unwrap();
    for i in 0..ELEMENTS {
        let (key, value) = gen_pair(&mut rng);
        let mut entry =
            morsdb::KvEntry::new(key.to_vec().into(), value.to_vec().into());
        entry.set_value_threshold(100_000);
        txn.set_entry(entry)
            .map_err(|e| eprintln!("{:?}", e))
            .unwrap();

        if i % 10_000 == 0 {
            txn.commit().unwrap();
            txn = db.begin_write().unwrap();
        }
    }
    txn.commit().unwrap();

    let end = Instant::now();
    let duration = end - start;
    println!(
        "Bulk loaded {} items in {}ms",
        ELEMENTS,
        duration.as_millis()
    );

    let txn = db.begin_read().unwrap();
    {
        {
            let start = Instant::now();
            // let len = txn.get_reader().len();
            // assert_eq!(len, ELEMENTS as u64 + 100_000 + 100);
            let end = Instant::now();
            let duration = end - start;
            println!("len() in {}ms", duration.as_millis());
        }

        for _ in 0..ITERATIONS {
            let mut rng = make_rng();
            let start = Instant::now();
            let mut checksum = 0u64;
            let mut expected_checksum = 0u64;
            for _ in 0..ELEMENTS {
                let (key, value) = gen_pair(&mut rng);
                let result = txn.get(key.to_vec().into()).unwrap();
                let v = result.value();
                checksum += v.as_ref()[0] as u64;
                expected_checksum += value[0] as u64;
            }
            assert_eq!(checksum, expected_checksum);
            let end = Instant::now();
            let duration = end - start;
            println!(
                "Random read {} items in {}ms",
                ELEMENTS,
                duration.as_millis()
            );
        }
    }
    drop(txn);

    for num_threads in [4, 8, 16, 32] {
        let mut rngs = make_rng_shards(num_threads, ELEMENTS);
        let start = Instant::now();

        thread::scope(|s| {
            for _ in 0..num_threads {
                let db2 = db.clone();
                let mut rng = rngs.pop().unwrap();
                s.spawn(move || {
                    let txn = db2.begin_read().unwrap();
                    let mut checksum = 0u64;
                    let mut expected_checksum = 0u64;
                    for _ in 0..(ELEMENTS / num_threads) {
                        let (key, value) = gen_pair(&mut rng);
                        let result = txn.get(key.to_vec().into()).unwrap();
                        let v = result.value();
                        checksum += v.as_ref()[0] as u64;
                        expected_checksum += value[0] as u64;
                    }
                    assert_eq!(checksum, expected_checksum);
                });
            }
        });

        let end = Instant::now();
        let duration = end - start;
        println!(
            "Random read ({} threads) {} items in {}ms",
            num_threads,
            ELEMENTS,
            duration.as_millis()
        );
    }
}
#[cfg(not(feature = "sync"))]
async fn benchmark(db: Mors) {
    let mut rng = make_rng();

    let start = Instant::now();
    let mut txn = db.begin_write().await.unwrap();
    for i in 0..ELEMENTS {
        let (key, value) = gen_pair(&mut rng);
        let mut entry =
            morsdb::KvEntry::new(key.to_vec().into(), value.to_vec().into());
        entry.set_value_threshold(100_000);
        txn.set_entry(entry)
            .map_err(|e| eprintln!("{:?}", e))
            .unwrap();

        if i % 10_000 == 0 {
            txn.commit().await.unwrap();
            txn = db.begin_write().await.unwrap();
        }
    }
    txn.commit().await.unwrap();

    let end = Instant::now();
    let duration = end - start;
    println!(
        "Bulk loaded {} items in {}ms",
        ELEMENTS,
        duration.as_millis()
    );

    let txn = db.begin_read().await.unwrap();
    {
        {
            let start = Instant::now();
            // let len = txn.get_reader().len();
            // assert_eq!(len, ELEMENTS as u64 + 100_000 + 100);
            let end = Instant::now();
            let duration = end - start;
            println!("len() in {}ms", duration.as_millis());
        }

        for _ in 0..ITERATIONS {
            let mut rng = make_rng();
            let start = Instant::now();
            let mut checksum = 0u64;
            let mut expected_checksum = 0u64;
            for _ in 0..ELEMENTS {
                let (key, value) = gen_pair(&mut rng);
                let result = txn.get(key.to_vec().into()).await.unwrap();
                let v = result.value();
                checksum += v.as_ref()[0] as u64;
                expected_checksum += value[0] as u64;
            }
            assert_eq!(checksum, expected_checksum);
            let end = Instant::now();
            let duration = end - start;
            println!(
                "Random read {} items in {}ms",
                ELEMENTS,
                duration.as_millis()
            );
        }
    }
    drop(txn);

    for num_threads in [4, 8, 16, 32] {
        let mut rngs = make_rng_shards(num_threads, ELEMENTS);
        let start = Instant::now();
        let db2 = db.clone();
        tokio::spawn(async move {
            let mut task = Vec::new();
            for _ in 0..num_threads {
                let db3 = db2.clone();
                let mut rng = rngs.pop().unwrap();
                let handle = tokio::spawn(async move {
                    let txn = db3.begin_read().await.unwrap();
                    let mut checksum = 0u64;
                    let mut expected_checksum = 0u64;
                    for _ in 0..(ELEMENTS / num_threads) {
                        let (key, value) = gen_pair(&mut rng);
                        let result =
                            txn.get(key.to_vec().into()).await.unwrap();
                        let v = result.value();
                        checksum += v.as_ref()[0] as u64;
                        expected_checksum += value[0] as u64;
                    }
                    assert_eq!(checksum, expected_checksum);
                });
                task.push(handle);
            }
            for t in task {
                t.await.unwrap();
            }
        })
        .await
        .unwrap();

        let end = Instant::now();
        let duration = end - start;
        println!(
            "Random read ({} threads) {} items in {}ms",
            num_threads,
            ELEMENTS,
            duration.as_millis()
        );
    }
}
