use std::{fs::create_dir, path::PathBuf};

use log::LevelFilter;

use morsdb::MorsBuilder;
use morsdb::Result;

#[cfg(not(feature = "sync"))]
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let mut logger = env_logger::builder();
    logger.filter_level(LevelFilter::Trace);
    logger.init();

    if let Err(e) = main_impl().await {
        eprintln!("Error: {:?}", e.to_string());
    }
}
#[cfg(not(feature = "sync"))]
async fn main_impl() -> Result<()> {
    let path = "./data/";
    let dir = PathBuf::from(path);
    if !dir.exists() {
        create_dir(&dir).unwrap();
    }

    let mut builder = MorsBuilder::default();
    builder.set_dir(dir).set_read_only(false);
    builder.set_num_memtables(1).set_memtable_size(1024 * 1024);
    let mors = builder.build().await?;

    Ok(())
}
#[cfg(feature = "sync")]
fn main() {
    let mut logger = env_logger::builder();
    logger.filter_level(LevelFilter::Trace);
    logger.init();
    if let Err(e) = main_impl() {
        eprintln!("Error: {:?}", e.to_string());
    };
}
#[cfg(feature = "sync")]
fn main_impl() -> Result<()> {
    let path = "./data/";
    let dir = PathBuf::from(path);
    if !dir.exists() {
        create_dir(&dir).unwrap();
    }
    let mut builder = MorsBuilder::default();
    builder.set_dir(dir).set_read_only(false);
    let mors = builder.build()?;
    let mut write_txn = mors.begin_write().unwrap();
    write_txn.set("key".into(), "value".into())?;
    write_txn.commit().unwrap();
    Ok(())
}
