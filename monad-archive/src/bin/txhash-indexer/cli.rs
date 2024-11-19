use clap::{Args, Parser, Subcommand, ValueEnum};
use monad_archive::{
    errors::ArchiveError,
    kv_interface::{kv_store_from_args, S3Store, Store, StoreInner},
};
// use monad_archive::cli_base::{S3Args, StorageType};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "monad-txhash-indexer", about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    pub triedb_path: Option<PathBuf>,

    #[command(flatten)]
    pub aws: Option<S3Args>,

    #[arg(long)]
    pub dynamodb_table: String,

    #[arg(long)]
    pub fs_dir: Option<PathBuf>,

    #[arg(long, default_value = "S3")]
    pub block_storage: StorageType,
}

#[derive(Debug, ValueEnum, Clone)]
pub enum StorageType {
    /// Use S3 storage
    S3,
    /// Use in-memory storage
    Memory,
    /// Use filesystem storage
    Fs,
}

#[derive(Args, Clone, Debug)]
#[group(required = true, multiple = false)]
pub struct S3Args {
    pub bucket: String,

    #[arg(long)]
    pub region: Option<String>,

    #[arg(long, default_value_t = 25_000_000)]
    pub max_response_size: u32,

    #[arg(long, default_value_t = 500)]
    pub max_concurrent_connections: usize,

    #[arg(long, default_value_t = 5)]
    pub max_concurrent_blocks: usize,
}

pub async fn store_from_args(cli: &Cli) -> Result<Store, ArchiveError> {
    kv_store_from_args(match cli.block_storage {
        StorageType::S3 => {
            let aws = cli.aws.as_ref().unwrap().clone();
            let s3_args = monad_archive::cli_base::S3Args {
                bucket: aws.bucket,
                region: aws.region,
                max_response_size: aws.max_response_size,
                max_concurrent_connections: aws.max_concurrent_connections,
                max_concurrent_blocks: aws.max_concurrent_blocks,
            };
            monad_archive::cli_base::StorageType::S3(s3_args)
        }
        StorageType::Memory => monad_archive::cli_base::StorageType::Memory,
        StorageType::Fs => monad_archive::cli_base::StorageType::Fs {
            dir: cli.fs_dir.as_ref().unwrap().clone(),
        },
    })
    .await
}
