use std::io::{BufRead, BufReader, BufWriter};

use clap::{Command, Parser, Subcommand};
use futures::stream::StreamExt;
use monad_archive::{
    cli::ArchiveArgs,
    fault::{BlockCheckResult, Fault},
    prelude::*,
};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{filter::Directive, EnvFilter};

#[derive(Parser, Debug)]
pub struct FindRangesArgs {
    #[arg(long)]
    pub faults_path: PathBuf,

    #[arg(long, default_value = "ranges.json")]
    pub ranges_out_path: PathBuf,

    #[arg(long, default_value = "group_ranges.json")]
    pub group_ranges_out_path: PathBuf,
}

pub async fn find_ranges(args: FindRangesArgs) -> Result<()> {
    // Open Faults file
    let file = BufReader::new(std::fs::File::open(args.faults_path)?);
    let mut lines = file.lines();

    info!("Parse lines in file");
    let mut block_check_results = Vec::new();
    while let Some(line) = lines.next() {
        let line = line?;
        let check: BlockCheckResult = serde_json::from_str(&line)?;
        block_check_results.push(check);
    }

    info!("Sort by block number");
    block_check_results.sort_by_key(|check| check.block_num);

    info!("Parition by replica and create list of contiguous ranges of missing data");
    let mut missing_block_ranges_by_group: HashMap<String, Vec<RangeInclusive<u64>>> =
        HashMap::new();
    let mut missing_block_ranges_by_replica: HashMap<String, Vec<RangeInclusive<u64>>> =
        HashMap::new();
    for check in block_check_results {
        for fault in check.faults {
            let mut buckets = match fault {
                Fault::S3MissingBlock { buckets } => buckets,
                Fault::S3MissingTraces { buckets } => buckets,
                Fault::S3MissingReceipts { buckets } => buckets,
                _ => {
                    continue;
                }
            };

            buckets.sort();
            let group = buckets.join(" ");
            let ranges = missing_block_ranges_by_group.entry(group).or_default();
            create_ranges(ranges, check.block_num);

            for bucket in buckets {
                let ranges = missing_block_ranges_by_replica.entry(bucket).or_default();
                create_ranges(ranges, check.block_num);
            }
        }
    }

    info!("Write ranges to file");
    serde_json::to_writer_pretty(
        BufWriter::new(std::fs::File::create(args.ranges_out_path)?),
        &missing_block_ranges_by_replica,
    )?;

    info!("Write group ranges to file");
    serde_json::to_writer_pretty(
        BufWriter::new(std::fs::File::create(args.group_ranges_out_path)?),
        &missing_block_ranges_by_group,
    )?;

    for (replica, ranges) in &missing_block_ranges_by_replica {
        info!(replica);
        let mut ranges = ranges.clone();
        ranges.sort_by_key(|r| u64::MAX - (r.end() - r.start()));
        let largest_gaps = ranges
            .iter()
            .take(10)
            .map(|r| r.end() - r.start())
            .collect::<Vec<_>>();

        info!(larges_ranges = ?&ranges[0..10.min(ranges.len())], ?largest_gaps);
    }

    let most = missing_block_ranges_by_group
        .keys()
        .max_by_key(|s| s.len())
        .unwrap();
    info!(most, group = ?missing_block_ranges_by_group.get(most).unwrap());

    Ok(())
}

pub fn create_ranges(ranges: &mut Vec<RangeInclusive<u64>>, block_num: u64) {
    match ranges.last_mut() {
        Some(range) if *range.end() == block_num => {
            // Do nothing, block_num already in range
        }
        Some(range) if *range.end() + 1 == block_num => {
            // Extend range
            *range = *range.start()..=block_num;
        }
        // Create a new range
        _ => ranges.push(block_num..=block_num),
    }
}
