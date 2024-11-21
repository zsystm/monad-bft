#![allow(unused_imports)]

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use archive_interface::{ArchiveWriterInterface, LatestKind::*};
use clap::Parser;
use dynamodb::DynamoDBArchive;
use eyre::Result;
use futures::{executor::block_on, future::join_all};
use monad_archive::*;
use s3_archive::{get_aws_config, S3Archive, S3ArchiveWriter};
use tokio::{join, sync::Semaphore, time::sleep};
use tracing::{error, info, warn, Level};

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();

    let concurrent_block_semaphore = Arc::new(Semaphore::new(args.max_concurrent_connections));

    // Construct s3 and dynamodb connections
    let sdk_config = get_aws_config(args.region).await;
    let dynamodb_archive =
        DynamoDBArchive::new(args.db_table, &sdk_config, args.max_concurrent_connections);
    let s3_archive = S3Archive::new(args.s3_bucket, &sdk_config).await?;
    let archive = S3ArchiveWriter::new(s3_archive).await?;

    // for testing
    if args.reset_index {
        archive.update_latest(0, Indexed).await?;
    }

    {
        let latest_indexed = archive.get_latest(Indexed).await.unwrap_or(0);
        let latest_uploaded = archive.get_latest(Uploaded).await.unwrap_or(0);

        info!("Latest indexed block is : {latest_indexed}");
        info!("Latest uploaded block is : {latest_uploaded}");
    }

    loop {
        sleep(Duration::from_millis(100)).await;
        let start = Instant::now();

        // get latest uploaded and indexed from s3
        let (uploaded, indexed) = join!(archive.get_latest(Uploaded), archive.get_latest(Indexed));
        let latest_uploaded = match uploaded {
            Ok(number) => number,
            Err(e) => {
                warn!("Error getting latest uploaded block: {e}");
                continue;
            }
        };
        let latest_indexed = indexed.unwrap_or(0);

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
            let archive = archive.clone();
            let dynamodb_archive = dynamodb_archive.clone();

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
            let on_err = |e: String| {
                let archive = archive.clone();
                async move {
                    if current_join_block != 0 {
                        info!(current_join_block, "Updating latest");
                        if let Err(e) = archive.update_latest(current_join_block - 1, Indexed).await
                        {
                            error!(
                                "Failed to update latest indexed s3 object, continuing. Error: {e}"
                            );
                            return false;
                        };
                    }
                    error!(
                        current_join_block,
                        latest_uploaded, start_block_number, "Error indexing block: {e}"
                    );
                    true
                }
            };
            match block_result {
                Ok(Ok(num_txs)) => {
                    current_join_block += 1;
                    num_txs_indexed += num_txs
                }
                Ok(Err(e)) => {
                    if !on_err(e.to_string()).await {
                        continue;
                    }
                }
                Err(e) => {
                    if !on_err(e.to_string()).await {
                        continue;
                    }
                }
            }
        }

        archive.update_latest(end_block_number, Indexed).await?;

        let duration = start.elapsed();
        info!(num_txs_indexed, "Time spent = {:?}", duration);
    }
}

async fn handle_block(
    archive: &S3ArchiveWriter,
    dynamodb: DynamoDBArchive,
    block_num: u64,
) -> Result<usize> {
    let block = archive.read_block(block_num).await?;
    info!(num_txs = block.body.len(), block_num, "block");

    let hashes = block.body.iter().map(|tx| tx.hash());

    dynamodb.index_block(hashes.collect(), block.number).await?;

    // check 1 key
    if let Some(tx) = block.body.first() {
        let key = tx.hash.to_string();
        tokio::spawn(async move {
            match dynamodb.get_block_number_by_tx_hash(&key).await {
                Ok(resp) => {
                    if resp != Some(block_num) {
                        warn!(key, resp, "Returned mapping not as expected");
                    } else {
                        info!(key, resp, "Check successful")
                    }
                }
                Err(e) => warn!("Error while checking: {e}"),
            };
        });
    }

    Ok(block.body.len())
}
