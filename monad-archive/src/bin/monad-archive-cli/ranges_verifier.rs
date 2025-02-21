use std::io::{BufReader, BufWriter};

use aws_sdk_s3::Client;
use clap::Parser;
use dashmap::DashMap;
use futures::future::{join_all, try_join_all};
use monad_archive::{cli::get_aws_config, prelude::*};

use crate::find_ranges::create_ranges;

#[derive(Parser, Debug)]
pub struct RangesVerifierArgs {
    #[arg(long, default_value = "ranges.json")]
    pub ranges_path: PathBuf,

    #[arg(long, default_value = "verified_ranges.json")]
    pub verified_ranges: PathBuf,
}

const BLOCK_PREFIX_STRIP: &str = "block/";

pub async fn verify_ranges_cli(args: RangesVerifierArgs) -> Result<()> {
    let file = BufReader::new(std::fs::File::open(args.ranges_path)?);
    let missing_block_ranges: HashMap<String, Vec<RangeInclusive<u64>>> =
        serde_json::from_reader(file)?;

    let s3_client = Client::new(&get_aws_config(Some("us-east-2".to_owned())).await);
    let readers: HashMap<String, BlockDataArchive> = missing_block_ranges
        .keys()
        .map(|replica| {
            (
                replica.clone(),
                BlockDataArchive::new(S3Bucket::from_client(
                    replica.clone(),
                    s3_client.clone(),
                    Metrics::none(),
                )),
            )
        })
        .collect();
    let verified_ranges = verify_ranges(missing_block_ranges, &readers).await?;

    info!("Write ranges to file");
    serde_json::to_writer_pretty(
        BufWriter::new(std::fs::File::create(args.verified_ranges)?),
        &verified_ranges,
    )?;

    Ok(())
}

pub async fn verify_ranges(
    missing_block_ranges: HashMap<String, Vec<RangeInclusive<u64>>>,
    readers: &HashMap<String, BlockDataArchive>,
) -> Result<HashMap<String, Vec<RangeInclusive<u64>>>> {
    try_join_all(missing_block_ranges.into_iter().map(|(replica, ranges)| {
        let reader = readers.get(&replica).unwrap().clone();
        tokio::spawn(async move {
            let total_unverified = ranges
                .iter()
                .fold(0, |acc, range| acc + 1 + range.end() - range.start());

            let missing = verify_range(ranges, &reader).await?;

            debug!(
                total_verified = missing
                    .iter()
                    .fold(0, |acc, r| acc + 1 + r.end() - r.start()),
                total_unverified, replica
            );
            Ok((replica, missing))
        })
    }))
    .await?
    .into_iter()
    .collect::<Result<HashMap<_, _>>>()
}

pub async fn verify_range(
    ranges: Vec<RangeInclusive<u64>>,
    reader: &BlockDataArchive,
) -> Result<Vec<RangeInclusive<u64>>> {
    let mut missing = Vec::new();
    let replica = reader.get_bucket();

    for range in ranges {
        let start = reader.block_key(*range.start());
        let end = reader.block_key(*range.end());
        let common = start
            .chars()
            .zip(end.chars())
            .take_while(|(a, b)| a == b)
            .map(|(a, _)| a)
            .collect::<String>();
        debug!("Common: {common}, start: {start}, end: {end}, replica: {replica}");

        let objects = reader
            .store
            .scan_prefix(&common)
            .await?
            .into_iter()
            .map(|key| {
                let block_str = key.strip_prefix(BLOCK_PREFIX_STRIP).unwrap();
                u64::from_str_radix(block_str, 10).unwrap()
            })
            .collect::<HashSet<u64>>();

        if range.end() - range.start() < objects.len() as u64 {
            debug!(
                found = objects.len(),
                range_len = range.end() - range.start()
            );
        }

        for block in range.to_owned() {
            if !objects.contains(&block) {
                create_ranges(&mut missing, block);
            }
        }
    }

    Ok(missing)
}

fn group_to_replica_names(group: &String) -> impl Iterator<Item = &str> {
    group.split_ascii_whitespace()
}
