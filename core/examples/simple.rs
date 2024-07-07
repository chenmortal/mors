use core::MorsBuilder;
use std::{fs::create_dir, path::PathBuf};

use core::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // create_dir(DEFAULT_DIR)?;
    let path = "./data/";
    let dir = PathBuf::from(path);
    if !dir.exists() {
        create_dir(&dir)?;
    }
    let mut builder = MorsBuilder::default();
    builder.set_dir(dir).set_read_only(false);
    let mors = builder.build().await?;
    Ok(())
    // println!("Hello, world!");
}
