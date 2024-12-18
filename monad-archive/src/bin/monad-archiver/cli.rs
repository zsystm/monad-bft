use std::path::PathBuf;

use clap::Parser;
use monad_archive::{ArchiveArgs, BlockDataReaderArgs};

#[derive(Debug, Parser)]
#[command(name = "monad-archive", about, long_about = None)]
pub struct Cli {
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    pub block_data_source: BlockDataReaderArgs,

    #[arg(long, value_parser = clap::value_parser!(ArchiveArgs))]
    pub archive_sink: ArchiveArgs,

    #[arg(long)]
    pub triedb_path: PathBuf,

    /// Set the max concurrent requests for triedb reads (smaller than monad-rpc)
    #[arg(long, default_value_t = 5_000)]
    pub triedb_max_concurrent_requests: u32,

    /// Set the max response size in bytes (the same as monad-rpc)
    #[arg(long, default_value_t = 25_000_000)]
    pub max_response_size: u32,

    /// Set max block processed per iter
    #[arg(long, default_value_t = 100)]
    pub max_blocks_per_iteration: u64,

    #[arg(long, default_value_t = 20)]
    pub max_concurrent_blocks: usize,

    #[arg(long)]
    pub otel_endpoint: Option<String>,
}
