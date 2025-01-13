#![allow(async_fn_in_trait)]

use std::{ops::RangeInclusive, time::Instant};

use clap::Parser;
use eyre::Result;
use futures::{StreamExt, TryStreamExt};
use metrics::Metrics;
use monad_archive::*;
use tokio::{
    time::{sleep, Duration},
    try_join,
};
use tracing::{error, info, warn, Level};
use workers::{bft_block_archiver::bft_block_archive_worker, block_archive_worker::archive_worker};

mod cli;
mod wal_checkpoint;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();
    info!(?args);

    let metrics = Metrics::new(
        args.otel_endpoint,
        "monad-archiver",
        Duration::from_secs(15),
    )?;

    let block_data_source = args.block_data_source.build(&metrics).await?;
    let archive_writer = args.archive_sink.build_block_data_archive(&metrics).await?;

    if let Some(path) = args.bft_block_path {
        info!("Spawning bft block archive worker...");
        tokio::spawn(bft_block_archive_worker(
            archive_writer.store.clone(),
            args.bft_block_header_path.unwrap_or(path.clone()),
            path,
            Duration::from_secs(args.bft_block_poll_freq_secs),
            metrics.clone(),
        ));
    }

    if let Some(path) = args.wal_path {
        info!("Spawning wal checkpoint worker...");
        tokio::spawn(wal_checkpoint_worker(
            archive_writer.store.clone(),
            path,
            Duration::from_secs(args.wal_checkpoint_freq_secs),
        ));
    }

    tokio::spawn(archive_worker(
        block_data_source,
        archive_writer,
        args.max_blocks_per_iteration,
        args.max_concurrent_blocks,
        args.start_block,
        metrics,
    ))
    .await
    .map_err(Into::into)
}
