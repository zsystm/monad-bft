use std::{
    collections::VecDeque,
    io::{BufReader, BufWriter},
};

use aws_sdk_s3::Client;
use clap::Parser;
use dashmap::DashMap;
use futures::future::{join_all, try_join_all};
use monad_archive::{cli::get_aws_config, fault::get_timestamp, kvstore::retry, prelude::*};
use opentelemetry::{KeyValue, Value};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::{
    find_ranges::create_ranges,
    ranges_verifier::{verify_range, verify_ranges},
};

#[derive(Parser, Debug)]
pub struct CheckerArgs {
    #[arg(long)]
    buckets: Vec<String>,

    // todo: allow configurable regions
    #[arg(long, default_value = "missing_ranges.txt")]
    missing_ranges_path: PathBuf,

    #[arg(long, default_value_t = 300)]
    check_frequency_secs: u64,

    #[arg(long, default_value = "checker_state.json")]
    checker_state_path: PathBuf,

    #[arg(long)]
    pub otel_endpoint: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplicaCheckerState {
    pub replica: String,
    pub start_checked: u64,
    pub end_checked: u64,
    pub missing_ranges: Vec<RangeInclusive<u64>>,
    pub total_missing: u64,
    pub last_checked_timestamp: String,
}

pub type CheckerState = HashMap<String, ReplicaCheckerState>;

pub async fn checker(args: CheckerArgs) -> Result<()> {
    let metrics = Metrics::new(
        args.otel_endpoint,
        "monad-archive-checker",
        args.buckets.join("+"),
        Duration::from_secs(15),
    )?;

    // scan replicas for missing blocks on startup
    let s3_client = Client::new(&get_aws_config(Some("us-east-2".to_owned())).await);

    let mut state = try_join_all(args.buckets.into_iter().map(|bucket| {
        let bucket = S3Bucket::from_client(bucket, s3_client.clone(), metrics.clone());
        let reader = BlockDataArchive::new(bucket);

        async move {
            match reader.get_latest(LatestKind::Uploaded).await {
                Ok(latest) => scan_replica_for_missing_blocks(&reader, 0, latest).await,
                Err(e) => Err(e),
            }
        }
    }))
    .await?
    .into_iter()
    .map(|s| (s.replica.clone(), s))
    .collect::<CheckerState>();

    // Log missing ranges
    state
        .values()
        .for_each(ReplicaCheckerState::log_missing_ranges);

    loop {
        tokio::time::sleep(Duration::from_secs(args.check_frequency_secs)).await;

        // scan latest-2k...latest-1k blocks for gaps

        for (replica, existing_state) in &mut state {
            let bucket =
                S3Bucket::from_client(replica.to_owned(), s3_client.clone(), metrics.clone());
            let reader = BlockDataArchive::new(bucket);
            let latest = reader.get_latest(LatestKind::Uploaded).await?;

            let start = latest - 2000;
            let end = latest - 1000;

            let recent_missing = scan_replica_for_missing_blocks(&reader, start, end).await?;

            let unioned_ranges = union_ranges(
                &existing_state.missing_ranges,
                &recent_missing.missing_ranges,
            );
            let verified_ranges = verify_range(unioned_ranges, &reader).await?;

            existing_state.missing_ranges = verified_ranges;
            existing_state.end_checked = existing_state.end_checked.max(end);
            existing_state.start_checked = existing_state.start_checked.min(start);
            existing_state.last_checked_timestamp = get_timestamp();
            existing_state.total_missing = existing_state
                .missing_ranges
                .iter()
                .fold(0, |acc, r| acc + 1 + r.end() - r.start());

            metrics.gauge_with_attrs(
                "missing_blocks",
                existing_state.total_missing,
                &[KeyValue::new("replica", replica.clone())],
            );
            metrics.gauge_with_attrs(
                "missing_blocks_latest_checked",
                existing_state.end_checked,
                &[KeyValue::new("replica", replica.clone())],
            );
        }
        metrics.gauge(
            "total_missing_blocks",
            state.values().map(|s| s.total_missing).sum(),
        );

        if let Err(e) = write_state_to_file(&args.checker_state_path, &state).await {
            error!(
                ?e,
                path = args.checker_state_path.to_string_lossy().to_string(),
                "Failed to write checker state to disk"
            );
        }
    }
}

async fn write_state_to_file(checker_state_path: &Path, state: &CheckerState) -> Result<()> {
    // Dump state to file
    let mut file = tokio::fs::File::options()
        .create(true)
        .write(true)
        .truncate(false)
        .open(checker_state_path)
        .await?;
    // Must serilize to a buffer since AsyncWrite is not Write
    let bytes = serde_json::to_vec(&state)?;
    file.write_all(&bytes).await?;
    file.flush().await?;

    Ok(())
}

pub async fn scan_replica_for_missing_blocks(
    reader: &BlockDataArchive,
    start: u64,
    end: u64,
) -> Result<ReplicaCheckerState> {
    let end_prefix_idx = end / 100_000;
    let start_prefix_idx = start / 100_000;
    let replica = reader.get_bucket();

    info!(
        replica,
        start = format_with_commas(start),
        end = format_with_commas(end),
        end_prefix_idx,
        "Scanning replica for missing blocks"
    );

    let results = futures::stream::iter(start_prefix_idx..=end_prefix_idx)
        .map(|idx| {
            let reader = &reader;
            let mut prefix = reader.block_key(idx * 100_000);
            let _ = prefix.split_off(prefix.len() - 5);

            debug!(replica, "Checking prefix {prefix}");

            retry(move || {
                let prefix = prefix.clone();
                async move { reader.store.scan_prefix(prefix.as_ref()).await }
            })
        })
        .buffered(15)
        .collect::<Vec<Result<Vec<String>>>>()
        .await;

    let present = results
        .into_iter()
        .try_fold(HashSet::new(), |mut acc, res| -> Result<_> {
            acc.extend(res?.iter().map(parse_block_num_from_key));
            Ok(acc)
        })?;

    let missing_ranges = (start..end)
        .filter_map(|block_num| {
            if present.contains(&block_num) {
                None
            } else {
                Some(block_num)
            }
        })
        .fold(Vec::new(), |mut ranges, block_num| {
            create_ranges(&mut ranges, block_num);
            ranges
        });

    Ok(ReplicaCheckerState {
        total_missing: missing_ranges
            .iter()
            .fold(0, |acc, range| acc + 1 + range.end() - range.start()),
        missing_ranges,
        replica: replica.to_owned(),
        start_checked: start,
        end_checked: end,
        last_checked_timestamp: get_timestamp(),
    })
}

fn parse_block_num_from_key(key: impl AsRef<str>) -> u64 {
    let block_str = key.as_ref().strip_prefix("block/").unwrap();
    u64::from_str_radix(block_str, 10).unwrap()
}

impl ReplicaCheckerState {
    fn log_missing_ranges(&self) {
        let ranges = &self.missing_ranges;
        let replica = &self.replica;

        if ranges.is_empty() {
            info!(replica, "No blocks missing");
            return;
        }

        info!(
            num_ranges = ranges.len(),
            total_missing = ranges
                .iter()
                .fold(0, |acc, range| acc + 1 + range.end() - range.start()),
            replica,
            "Missing Ranges"
        );

        for range in ranges {
            debug!(
                len = 1 + range.end() - range.start(),
                replica,
                "Missing range: {} - {}",
                format_with_commas(range.start()),
                format_with_commas(range.end())
            );
        }
    }
}

use std::ops::RangeInclusive;

/// Merges overlapping or adjacent ranges into a vector of inclusive ranges.
pub fn union_ranges(
    a: &[RangeInclusive<u64>],
    b: &[RangeInclusive<u64>],
) -> Vec<RangeInclusive<u64>> {
    if a.is_empty() && b.is_empty() {
        return vec![];
    }

    let mut ranges = Vec::with_capacity(a.len().max(b.len()));
    ranges.extend_from_slice(a);
    ranges.extend_from_slice(b);
    ranges.sort_by_key(|r| *r.start());

    let mut result = Vec::new();
    let mut current_range = ranges[0].clone();

    for range in ranges.iter().skip(1) {
        let current_start = *current_range.start();
        let current_end = *current_range.end();
        let next_start = *range.start();
        let next_end = *range.end();

        // Check if ranges overlap or are adjacent
        if current_end + 1 >= next_start {
            // Merge the ranges
            current_range = current_start..=std::cmp::max(current_end, next_end);
        } else {
            // No overlap, push current range and start a new one
            result.push(current_range);
            current_range = range.clone();
        }
    }

    // Don't forget to push the last merged range
    result.push(current_range);

    result
}

fn format_with_commas(i: impl ToString) -> String {
    let i_str = i.to_string();
    let mut s = String::with_capacity(i_str.len() + i_str.len() / 3);

    for (idx, ch) in i_str.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            s.push(',');
        }
        s.push(ch);
    }

    s.chars().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for manual implementation
    #[test]
    fn test_manual_zero() {
        assert_eq!(format_with_commas(0), "0");
    }

    #[test]
    fn test_manual_small_number() {
        assert_eq!(format_with_commas(123), "123");
    }

    #[test]
    fn test_manual_thousand() {
        assert_eq!(format_with_commas(1000), "1,000");
    }

    #[test]
    fn test_manual_large_number() {
        assert_eq!(format_with_commas(1234567890), "1,234,567,890");
    }

    #[test]
    fn test_manual_negative_number() {
        assert_eq!(format_with_commas(-1234567), "-1,234,567");
    }

    #[test]
    fn test_both_empty() {
        let a: Vec<RangeInclusive<u64>> = vec![];
        let b: Vec<RangeInclusive<u64>> = vec![];
        let result = union_ranges(&a, &b);
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_one_empty() {
        let a = vec![1..=5, 7..=10];
        let b: Vec<RangeInclusive<u64>> = vec![];
        let result = union_ranges(&a, &b);
        assert_eq!(result, vec![1..=5, 7..=10]);

        let a: Vec<RangeInclusive<u64>> = vec![];
        let b = vec![2..=6, 8..=12];
        let result = union_ranges(&a, &b);
        assert_eq!(result, vec![2..=6, 8..=12]);
    }

    #[test]
    fn test_single_ranges() {
        let a = vec![1..=5];
        let b = vec![10..=15];
        let result = union_ranges(&a, &b);
        assert_eq!(result, vec![1..=5, 10..=15]);
    }

    #[test]
    fn test_non_overlapping_non_adjacent() {
        let a = vec![1..=5, 12..=15];
        let b = vec![7..=10, 20..=25];
        let result = union_ranges(&a, &b);
        assert_eq!(result, vec![1..=5, 7..=10, 12..=15, 20..=25]);
    }

    #[test]
    fn test_adjacent_ranges() {
        let a = vec![1..=5, 11..=15];
        let b = vec![6..=10, 16..=20];
        let result = union_ranges(&a, &b);
        assert_eq!(result, vec![1..=20]);
    }

    #[test]
    fn test_overlapping_ranges() {
        let a = vec![1..=5, 8..=12];
        let b = vec![3..=9, 10..=15];
        let result = union_ranges(&a, &b);
        assert_eq!(result, vec![1..=15]);
    }

    #[test]
    fn test_ranges_in_reverse_order() {
        let a = vec![7..=10, 1..=3];
        let b = vec![15..=20, 4..=6];
        let result = union_ranges(&a, &b);
        assert_eq!(result, vec![1..=10, 15..=20]);
    }

    #[test]
    fn test_completely_contained_ranges() {
        let a = vec![1..=10, 20..=30];
        let b = vec![3..=5, 22..=25];
        let result = union_ranges(&a, &b);
        assert_eq!(result, vec![1..=10, 20..=30]);
    }

    #[test]
    fn test_cross_slice_merging() {
        let a = vec![1..=5, 15..=20];
        let b = vec![6..=14, 25..=30];
        let result = union_ranges(&a, &b);
        assert_eq!(result, vec![1..=20, 25..=30]);
    }

    #[test]
    fn test_large_values() {
        let a = vec![1000..=5000, 15000..=20000];
        let b = vec![4500..=10000, 25000..=30000];
        let result = union_ranges(&a, &b);
        assert_eq!(result, vec![1000..=10000, 15000..=20000, 25000..=30000]);
    }

    #[test]
    fn test_identical_ranges() {
        let a = vec![1..=5, 10..=15];
        let b = vec![1..=5, 10..=15];
        let result = union_ranges(&a, &b);
        assert_eq!(result, vec![1..=5, 10..=15]);
    }

    #[test]
    fn test_same_start_different_end() {
        let a = vec![1..=5, 10..=20];
        let b = vec![1..=8, 10..=15];
        let result = union_ranges(&a, &b);
        assert_eq!(result, vec![1..=8, 10..=20]);
    }
}
