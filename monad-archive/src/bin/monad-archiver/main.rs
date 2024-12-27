#![allow(async_fn_in_trait)]

use std::{ops::RangeInclusive, time::Instant};

use clap::Parser;
use eyre::Result;
use futures::{join, StreamExt, TryStreamExt};
use metrics::Metrics;
use tokio::{
    time::{sleep, Duration},
    try_join,
};
use tracing::{error, info, warn, Level};

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

    let block_data_source = args.block_data_source.build(&metrics).await?;

    let archive_writer = args.archive_sink.build_block_data_archive(&metrics).await?;

    tokio::spawn(archive_worker(
        block_data_source,
        archive_writer,
        args.max_blocks_per_iteration,
        args.max_concurrent_blocks,
        metrics,
    ))
    .await
    .map_err(Into::into)
}

async fn archive_worker(
    block_data_source: (impl BlockDataReader + Sync),
    archive_writer: BlockDataArchive,
    max_blocks_per_iteration: u64,
    max_concurrent_blocks: usize,
    metrics: Metrics,
) {
    loop {
        sleep(Duration::from_millis(100)).await;

        // query latest
        let (latest_source, latest_uploaded) = join!(
            block_data_source.get_latest(LatestKind::Uploaded),
            archive_writer.get_latest(LatestKind::Uploaded)
        );

        let latest_source = match latest_source {
            Ok(number) => number,
            Err(e) => {
                warn!("Error getting latest source block: {e:?}");
                continue;
            }
        };
        let latest_uploaded = latest_uploaded.unwrap_or(0);

        metrics.gauge("source_latest_block_num", latest_source);
        metrics.gauge("latest_uploaded", latest_uploaded);

        if latest_source <= latest_uploaded {
            info!(latest_uploaded, latest_source, "Nothing to process");
            continue;
        }

        // compute start and end block
        let start_block_number = if latest_uploaded == 0 {
            0
        } else {
            latest_uploaded + 1
        };

        let end_block_number = latest_source.min(start_block_number + max_blocks_per_iteration - 1);
        metrics.gauge("end_block_number", end_block_number);

        info!(
            start = start_block_number,
            end = end_block_number,
            latest_source,
            latest_uploaded,
            "Archiving group of blocks",
        );

        archive_blocks(
            &block_data_source,
            start_block_number..=end_block_number,
            &archive_writer,
            max_concurrent_blocks,
        )
        .await;
    }
}

async fn archive_blocks(
    reader: &(impl BlockDataReader + Sync),
    range: RangeInclusive<u64>,
    archiver: &BlockDataArchive,
    concurrency: usize,
) {
    let start = Instant::now();

    let res: Result<(), u64> = futures::stream::iter(range.clone().into_iter())
        .map(|block_num: u64| async move {
            match archive_block(reader, block_num, archiver).await {
                Ok(_) => Ok(block_num),
                Err(e) => {
                    error!("Failed to handle block: {e:?}");
                    Err(block_num)
                }
            }
        })
        .buffered(concurrency)
        .then(|r| async move {
            if let Ok(block_num) = &r {
                if block_num % 10 == 0 {
                    checkpoint_latest(archiver, *block_num).await;
                }
            }
            r.map(|_| ())
        })
        .try_collect()
        .await;

    info!(
        elapsed = start.elapsed().as_millis(),
        start = range.start(),
        end = range.end(),
        "Finished archiving range",
    );

    let new_latest_uploaded = match res {
        Ok(()) => *range.end(),
        Err(err_block) => err_block - 1,
    };

    if new_latest_uploaded != 0 {
        checkpoint_latest(archiver, new_latest_uploaded).await;
    }
}

async fn archive_block(
    reader: &impl BlockDataReader,
    block_num: u64,
    archiver: &BlockDataArchive,
) -> Result<()> {
    let mut num_txs = None;

    try_join!(
        async {
            // NOTE: is it ok to error out here? Previous behavior returned Ok(())
            // with a warn if the header was not found.
            // That seems incorrect, but should investigate
            let block = reader.get_block_by_number(block_num).await?;
            num_txs = Some(block.body.len());
            archiver.archive_block(block).await
        },
        async {
            // let receipts = reader.get_block_receipts(block_num).await?;
            // archiver.archive_receipts(receipts, block_num).await
            Ok(())
        },
        async {
            let traces = reader.get_block_traces(block_num).await?;
            archiver.archive_traces(traces, block_num).await
        },
    )?;
    info!(block_num, num_txs, "Successfully archived block");
    Ok(())
}

async fn checkpoint_latest(archiver: &BlockDataArchive, block_num: u64) {
    match archiver
        .update_latest(block_num, LatestKind::Uploaded)
        .await
    {
        Ok(()) => info!(block_num, "Set latest uploaded checkpoint"),
        Err(e) => error!(block_num, "Failed to set latest uploaded block: {e:?}"),
    }
}
