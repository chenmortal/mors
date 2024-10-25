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
    // let mut write_tran = mors.begin_write().await?;
    // write_tran.set("key".into(), "value".into())?;
    // write_tran.commit().await?;
    let read_tran = mors.begin_read().await?;
    let v = read_tran.get("key".into()).await?;
    let vp = v.value().as_ref();
    dbg!(vp);
    assert_eq!(vp, b"value");
    // let v = write_tran.get("key".into()).await?;
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
    let v = write_txn.get("key".into())?;
    let vp = v.value().as_ref();
    assert_eq!(vp, b"value");
    write_txn.commit().unwrap();
    Ok(())
}
