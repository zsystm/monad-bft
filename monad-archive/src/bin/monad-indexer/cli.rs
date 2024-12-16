use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use monad_archive::StorageArgs;

#[derive(Debug, Parser)]
#[command(name = "monad-indexer", about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub storage: StorageArgs,

    /// Number of blocks to handle per loop iteration
    #[arg(long, default_value_t = 20)]
    pub max_blocks_per_iteration: u64,

    #[arg(long, default_value_t = false)]
    pub reset_index: bool,

    /// Block number to start at
    #[arg(long)]
    pub start_block: Option<u64>,

    #[arg(long)]
    pub otel_endpoint: Option<String>,
}
