#![allow(async_fn_in_trait)]

use std::{sync::Arc, time::Instant};

use clap::Parser;
use eyre::Result;
use futures::future::join_all;
use metrics::Metrics;
use tokio::{
    sync::Semaphore,
    time::{sleep, Duration},
    try_join,
};
use tracing::{error, info, Level};

use monad_archive::*;
mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();
    let metrics = Metrics::new(
        args.otel_endpoint,
        "monad-archiver",
        Duration::from_secs(15),
    )?;

    let concurrent_block_semaphore = Arc::new(Semaphore::new(args.max_concurrent_blocks));

    // This will spin off a polling thread
    let triedb = args.block_data_source.build(&metrics).await?;

    // Construct storage
    // let archive_writer = BlockDataArchive::new(args.storage.build_stores(metrics.clone()).await?.0);
    let archive_writer = args.archive_sink.build_block_data_archive(&metrics).await?;

    // Check for new blocks every 100 ms
    // Issue requests to triedb, poll data and push to relevant tables
    loop {
        sleep(Duration::from_millis(100)).await;

        let trie_db_block_number = triedb.get_latest(LatestKind::Uploaded).await?;

        metrics.gauge("triedb_block_number", trie_db_block_number);

        let latest_processed_block =
            (archive_writer.get_latest(LatestKind::Uploaded).await).unwrap_or_default();
        metrics.gauge("latest_uploaded", latest_processed_block);

        if trie_db_block_number <= latest_processed_block {
            info!(
                "Nothing to process. S3 archive progress: {}, triedb progress: {}",
                latest_processed_block, trie_db_block_number
            );
            continue;
        }

        let start_block_number = if latest_processed_block == 0 {
            0
        } else {
            latest_processed_block + 1
        };

        let end_block_number =
            trie_db_block_number.min(start_block_number + args.max_blocks_per_iteration - 1);
        metrics.gauge("end_block_number", end_block_number);

        let start = Instant::now();
        info!(
            "Processing blocks from {} to {}, start time = {:?}",
            start_block_number, end_block_number, start
        );

        let join_handles = (start_block_number..=end_block_number).map(|current_block: u64| {
            let triedb = triedb.clone();
            let archive_writer = archive_writer.clone();

            let semaphore = concurrent_block_semaphore.clone();
            tokio::spawn(async move {
                let _permit = semaphore
                    .acquire()
                    .await
                    .expect("Got permit to execute a new block");
                handle_block(&triedb, current_block, &archive_writer).await
            })
        });

        let block_results = join_all(join_handles).await;
        let mut current_join_block = start_block_number;
        for block_result in block_results {
            match block_result {
                Ok(Ok(())) => {
                    current_join_block += 1;
                }
                Ok(Err(e)) => {
                    error!(current_join_block, "Failure writing block, {e}");
                    break;
                }
                Err(e) => {
                    error!(current_join_block, "Failure writing block, {e}");
                    break;
                }
            }
        }

        if current_join_block != 0 {
            archive_writer
                .update_latest(current_join_block - 1, LatestKind::Uploaded)
                .await?;
        }

        let duration = start.elapsed();
        info!("Time spent = {:?}", duration);
    }
}

async fn handle_block(
    reader: &impl BlockDataReader,
    current_block: u64,
    archiver: &BlockDataArchive,
) -> Result<()> {
    try_join!(
        async {
            // NOTE: is it ok to error out here? Previous behavior returned Ok(())
            // with a warn if the header was not found.
            // This seems incorrect, but should investigate
            let block = reader.get_block_by_number(current_block).await?;
            archiver.archive_block(block).await
        },
        async {
            let receipts = reader.get_block_receipts(current_block).await?;
            archiver.archive_receipts(receipts, current_block).await
        },
        async {
            let traces = reader.get_block_traces(current_block).await?;
            archiver.archive_traces(traces, current_block).await
        },
    )?;
    info!("Successfully archived block {}", current_block);
    Ok(())
}
