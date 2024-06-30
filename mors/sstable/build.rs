use std::{io::Result, path::Path};


fn main() -> Result<()> {
    prost_build::Config::new()
        .out_dir("src/pb/")
        .compile_protos(&["src/pb/pb.proto"], &["src/"])?;
    flatc_rust::run(flatc_rust::Args {
        lang: "rust",  // `rust` is the default, but let's be explicit
        inputs: &[Path::new("src/fb/table.fbs")],
        out_dir: Path::new("src/fb/"),
        ..Default::default()
    })?;
    Ok(())
}
