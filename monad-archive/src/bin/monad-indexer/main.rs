#![allow(unused_imports)]

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use archive_reader::{ArchiveReader, LatestKind::*};
use archive_tx_index::TxIndexArchiver;
use clap::Parser;
use eyre::Result;
use futures::{executor::block_on, future::join_all};
use metrics::Metrics;
use monad_archive::*;
use tokio::{join, sync::Semaphore, time::sleep, try_join};
use tracing::{error, info, warn, Level};

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();

    // let concurrent_block_semaphore = Arc::new(Semaphore::new(args.max_concurrent_connections));
    let concurrent_block_semaphore = Arc::new(Semaphore::new(5));
    let metrics = Metrics::new(args.otel_endpoint, "monad-indexer", Duration::from_secs(15))?;

    let block_data_reader = args.block_data_source.build(&metrics).await?;
    let tx_index_archiver = args.archive_sink.build_index_archive(&metrics).await?;

    // for testing
    if args.reset_index {
        tx_index_archiver.update_latest_indexed(0).await?;
    }

    info!(
        "Latest indexed block :{}. Latest uploaded block :{}",
        block_data_reader.get_latest(Indexed).await.unwrap_or(0),
        block_data_reader.get_latest(Uploaded).await.unwrap_or(0)
    );

    let mut latest_indexed = if let Some(start_block) = args.start_block {
        start_block
    } else {
        block_data_reader.get_latest(Indexed).await.unwrap_or(0)
    };

    loop {
        sleep(Duration::from_millis(100)).await;
        let start = Instant::now();

        // get latest uploaded and indexed from s3
        let latest_uploaded = match block_data_reader.get_latest(Uploaded).await {
            Ok(number) => number,
            Err(e) => {
                warn!("Error getting latest uploaded block: {e:?}");
                continue;
            }
        };
        metrics.gauge("latest_uploaded", latest_uploaded);

        if latest_uploaded <= latest_indexed {
            info!(latest_indexed, latest_uploaded, "Nothing to process");
            continue;
        }

        // compute start and end block for this upload
        let start_block_number = if latest_indexed == 0 {
            0
        } else {
            latest_indexed + 1
        };
        let end_block_number =
            latest_uploaded.min(start_block_number + args.max_blocks_per_iteration);

        info!(
            start_block_number,
            end_block_number, latest_uploaded, "Spawning index uploads for blocks"
        );

        let join_handles = (start_block_number..=end_block_number).map(|current_block: u64| {
            let archive = block_data_reader.clone();
            let dynamodb_archive = tx_index_archiver.clone();

            let semaphore = concurrent_block_semaphore.clone();
            tokio::spawn(async move {
                let _permit = semaphore
                    .acquire()
                    .await
                    .expect("Got permit to execute a new block");
                handle_block(&archive, dynamodb_archive, current_block).await
            })
        });

        let mut num_txs_indexed = 0;
        let block_results = join_all(join_handles).await;
        let mut current_join_block = start_block_number;
        for block_result in block_results {
            // two match arm error conditions are similar but have different error types
            match block_result {
                Ok(Ok(num_txs)) => {
                    current_join_block += 1;
                    num_txs_indexed += num_txs
                }
                Ok(Err(e)) => {
                    error!(
                        current_join_block,
                        latest_uploaded, start_block_number, "Error indexing block: {e:?}"
                    );
                    break;
                }
                Err(e) => {
                    error!(
                        current_join_block,
                        latest_uploaded, start_block_number, "Error indexing block: {e:?}"
                    );
                    break;
                }
            }
        }

        latest_indexed = current_join_block.saturating_sub(1);
        tx_index_archiver
            .update_latest_indexed(latest_indexed)
            .await?;
        metrics.gauge("latest_indexed", latest_indexed);
        metrics.counter("txs_indexed", num_txs_indexed as u64);

        let duration = start.elapsed();
        info!(num_txs_indexed, "Time spent = {:?}", duration);
    }
}

async fn handle_block(
    block_data_reader: &impl BlockDataReader,
    tx_index_archiver: TxIndexArchiver,
    block_num: u64,
) -> Result<usize> {
    let (block, traces, receipts) = try_join!(
        block_data_reader.get_block_by_number(block_num),
        block_data_reader.get_block_traces(block_num),
        block_data_reader.get_block_receipts(block_num)
    )?;
    let num_txs = block.body.len();
    info!(num_txs, block_num, "Block");

    let first = block.body.first().cloned();
    let first_rx = receipts.first().cloned();
    let first_trace = traces.first().cloned();
    tx_index_archiver
        .index_block(block, traces, receipts)
        .await?;

    // check 1 key
    if let Some(tx) = first {
        let key = format!("{:x}", tx.hash);
        tokio::spawn(async move {
            match tx_index_archiver.store.get(&key).await {
                Ok(Some(resp)) => {
                    if resp.header_subset.block_number != block_num
                        || Some(&resp.receipt) != first_rx.as_ref()
                        || Some(&resp.trace) != first_trace.as_ref()
                    {
                        warn!(key, ?resp, "Returned mapping not as expected");
                    } else {
                        info!(
                            key,
                            resp_block_num = resp.header_subset.block_number,
                            "Check successful"
                        );
                    }
                }
                Ok(None) => warn!(key, block_num, "No key found for key"),
                Err(e) => warn!("Error while checking: {e}"),
            };
        });
    }

    Ok(num_txs)
}
