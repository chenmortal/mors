use core::MorsBuilder;
use std::{fs::create_dir, path::PathBuf};

use log::LevelFilter;

use core::Result;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    if let Err(e) = main_impl().await {
        eprintln!("Error: {:?}", e.to_string());
    }
}
async fn main_impl() -> Result<()> {
    let mut logger = env_logger::builder();
    logger.filter_level(LevelFilter::Trace);
    logger.init();

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
