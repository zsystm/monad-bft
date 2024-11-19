use std::{sync::Arc, time::Duration};

use archive_writer::ArchiveWriter;
use clap::Parser;
use cli::store_from_args;
use dynamodb::DynamoDBArchive;
use errors::ArchiveError;
use index::TxIndex;
use kv_interface::{kv_store_from_args, KVStore};
use monad_archive::*;
use tokio::{join, time::sleep, try_join};
use tracing::{error, info, warn};

mod cli;
mod dynamodb;
mod index;

pub type Result<T = ()> = std::result::Result<T, ArchiveError>;
pub type Archive = ArchiveWriter<kv_interface::Store>;

#[tokio::main]
async fn main() -> Result {
    let args = cli::Cli::parse();

    let store = store_from_args(&args).await?;
    let archive = ArchiveWriter::new(store.clone());
    let dynamodb_archive = DynamoDBArchive::new(&args).await?;
    // todo: combine TxIndex and dynamodb archive
    // let tx_index = TxIndex::new(store);

    // Construct an s3 instance
    let mut latest_indexed = archive.get_latest_indexed().await.unwrap_or(0);
    let mut latest_uploaded = archive.get_latest_uploaded().await.unwrap_or(0);

    info!("Latest indexed block is : {latest_indexed}");
    info!("Latest uploaded block is : {latest_uploaded}");

    loop {
        sleep(Duration::from_millis(10)).await;
        if let Err(e) = process(
            &archive,
            &dynamodb_archive,
            &mut latest_indexed,
            &mut latest_uploaded,
        )
        .await
        {
            error!(latest_indexed, latest_uploaded, "Error indexing block: {e}");
        }
    }
}

// TODO: fix case where last indexed doesn't exist
async fn process(
    //
    archive: &Archive,
    dynamodb: &DynamoDBArchive,
    last_indexed: &mut u64,
    last_uploaded: &mut u64,
) -> Result {
    let (latest_indexed, lastest_uploaded) =
        try_join!(archive.get_latest_indexed(), archive.get_latest_uploaded())?;

    refresh(last_indexed, latest_indexed);
    refresh(last_uploaded, lastest_uploaded);

    if last_indexed >= last_uploaded {
        return Ok(());
    }

    let block = archive.read_block(*last_indexed + 1).await?;

    let hashes = block.body.iter().map(|tx| tx.hash()).collect();
    dynamodb.index_block(hashes, block.number).await?;
    // index.upload_batch(tx_hashes, block.number).await?;

    Ok(())
}

// Note: does not make an effort to handle gaps. Another serivce will periodically scan and backfill indexes
fn refresh(local: &mut u64, fetched: u64) {
    *local = (*local).max(fetched);
}
