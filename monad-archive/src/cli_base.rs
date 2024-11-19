use clap::{Args, Parser, Subcommand};
use std::{path::PathBuf, sync::Arc};

use crate::{
    errors::ArchiveError,
    kv_interface::{FileStore, KVStore, MemoryStore, S3Store, Store},
};

#[derive(Debug, Subcommand)]
pub enum StorageType {
    /// Use S3 storage
    S3(S3Args),
    /// Use in-memory storage
    Memory,
    /// Use filesystem storage
    Fs {
        /// Directory path for storage
        dir: PathBuf,
    },
}

#[derive(Debug, Args, Clone)]
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
