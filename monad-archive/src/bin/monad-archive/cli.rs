use clap::{Args, Parser, Subcommand};
use monad_archive::cli_base::StorageType;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "monad-archive", about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    pub triedb_path: Option<PathBuf>,

    #[command(subcommand)]
    pub storage: StorageType,
}

impl Cli {
    pub fn max_concurrent_connections(&self) -> usize {
        match &self.storage {
            StorageType::S3(s3_args) => s3_args.max_concurrent_connections,
            StorageType::Memory => 5,
            StorageType::Fs { .. } => 5,
        }
    }
}
