#![allow(unused_imports)]

use std::io::{BufRead, BufReader, BufWriter};

use checker::CheckerArgs;
use clap::{Command, Parser, Subcommand};
use find_ranges::{find_ranges, FindRangesArgs};
use futures::stream::StreamExt;
use monad_archive::{
    cli::ArchiveArgs,
    fault::{BlockCheckResult, Fault},
    prelude::*,
};
use ranges_verifier::{verify_ranges_cli, RangesVerifierArgs};
use scan_missing_objects::ScanMissingObjectsArgs;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{filter::Directive, EnvFilter};

mod checker;
mod find_ranges;
mod ranges_verifier;
mod scan_missing_objects;

#[derive(Debug, Parser)]
#[command(name = "monad-archive-cli", about)]
pub struct Cli {
    #[command(subcommand)]
    mode: Modes,
}

#[derive(Subcommand, Debug)]
enum Modes {
    FindRanges(FindRangesArgs),
    RangesVerifier(RangesVerifierArgs),
    ScanMissingObjects(ScanMissingObjectsArgs),
    Checker(CheckerArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let args = Cli::parse();
    info!(?args);

    match args.mode {
        Modes::FindRanges(args) => find_ranges(args).await,
        Modes::RangesVerifier(args) => verify_ranges_cli(args).await,
        Modes::ScanMissingObjects(args) => scan_missing_objects::scan_missing_objects(args).await,
        Modes::Checker(args) => checker::checker(args).await,
    }
}
