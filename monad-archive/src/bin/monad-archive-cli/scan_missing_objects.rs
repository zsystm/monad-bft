use std::io::{BufReader, BufWriter};

use aws_sdk_s3::Client;
use clap::Parser;
use dashmap::DashMap;
use futures::future::{join_all, try_join_all};
use monad_archive::{cli::get_aws_config, kvstore::retry, prelude::*};

use crate::{
    checker::{scan_replica_for_missing_blocks, ReplicaCheckerState},
    find_ranges::create_ranges,
};

#[derive(Parser, Debug)]
pub struct ScanMissingObjectsArgs {
    #[arg(long)]
    buckets: Vec<String>,

    #[arg(long, default_value = "missing_ranges.txt")]
    missing_ranges_path: PathBuf,
}

pub async fn scan_missing_objects(args: ScanMissingObjectsArgs) -> Result<()> {
    let s3_client = Client::new(&get_aws_config(Some("us-east-2".to_owned())).await);

    let missing_ranges = try_join_all(args.buckets.into_iter().map(|bucket| {
        let bucket = S3Bucket::from_client(bucket, s3_client.clone(), Metrics::none());
        let reader = BlockDataArchive::new(bucket);

        tokio::spawn(async move {
            let latest = reader.get_latest(LatestKind::Uploaded).await?;
            scan_replica_for_missing_blocks(&reader, 0, latest).await
        })
    }))
    .await?
    .into_iter()
    .try_fold(HashMap::new(), |mut map, state| -> Result<_> {
        let state = state?;
        map.insert(state.replica.clone(), state);
        Ok(map)
    })?;

    info!("Write ranges to file");
    serde_json::to_writer_pretty(
        BufWriter::new(std::fs::File::create(args.missing_ranges_path)?),
        &missing_ranges,
    )?;

    Ok(())
}
