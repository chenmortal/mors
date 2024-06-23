use std::io::Result;

use prost_build;

fn main() -> Result<()> {
    prost_build::Config::new()
        .out_dir("src/manifest/")
        .compile_protos(&["src/manifest/manifest.proto"], &["src/"])?;
    Ok(())
}
