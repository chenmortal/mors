// use std::fs::create_dir;
use clap::Parser;
use clap::Subcommand;
use mors_levelctl::manifest::error::ManifestError;
use mors_levelctl::manifest::ManifestBuilder;
use mors_traits::default::DEFAULT_DIR;
// use tabled::builder::Builder;
// use clap::ValueEnum;

use std::path::PathBuf;
// use morsdb::MorsBuilder;

#[derive(Parser)]
#[command(name = "morscli")]
#[command(version = "0.1.0")]
#[command(about = "A simple CLI for MorsDB")]
struct Cli {
    // /// The directory to store the data or metadata
    // #[arg(short,long,default_value=DEFAULT_DIR)]
    // dir: PathBuf,
    #[command(subcommand)]
    command: Option<Commands>,
}
#[derive(Subcommand)]
enum Commands {
    // /// Print some information
    // Print {
    //     /// Print the manifest file
    //     #[arg(short, long)]
    //     manifest: bool,
    //     // /// Print the KMS file
    //     // #[arg(short, long)]
    //     // kms: bool,
    // },
    // Test {
    //     #[command(subcommand)]
    //     cmd: Option<TestSubCmd>,
    // },
    PrintManifest {
        #[arg(short, long, default_value = DEFAULT_DIR)]
        dir: PathBuf,
    },
}
// #[derive(Subcommand, Clone)]
// enum TestSubCmd {
//     Test,
// }
#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Some(command) = cli.command {
        match command {
            Commands::PrintManifest { dir } => {
                match handle_print_manifest(dir).await {
                    Ok(_) => {}
                    Err(e) => {
                        eprint!("{}", e);
                    }
                };
            }
        }
    }
}
async fn handle_print_manifest(dir: PathBuf) -> Result<(), ManifestError> {
    let mut builder = ManifestBuilder::default();
    builder.set_dir(dir);
    builder.set_read_only(true);
    let manifest = builder.build()?;
    let manifest_inner = manifest.lock().await;
    let info = manifest_inner.info();
    println!("{}", info);
    Ok(())
}

#[test]
fn test_tabled() {
    // let mut builder = Builder::new();
}
// #[tokio::main]
// async fn main() {
//     let path = "../data/";
//     let dir = PathBuf::from(path);
//     if !dir.exists() {
//         create_dir(&dir).unwrap();
//     }
//     let mut builder = MorsBuilder::default();
//     builder.set_dir(dir).set_read_only(false);
//     builder
//         .set_num_memtables(5)
//         .set_memtable_size(64 * 1024 * 1024);
//     let mors = builder.build().await.unwrap();
// }
