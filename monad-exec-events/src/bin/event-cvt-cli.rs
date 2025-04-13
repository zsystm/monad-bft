//! A command line interface to run instances of the execution event
//! "cross-validation test" (CVT)
//!
//! A #[test] within exec_event_test_util.rs performs a specific instance of
//! the CVT test using fixed input data; this command line utility allows
//! you to re-run a CVT test with arbitrary inputs.

use std::{path::PathBuf, process};

use clap::Parser;
use monad_event_ring::{event_ring::EventRing, event_ring_util::EventRingSnapshot};
use monad_exec_events::{exec_event_test_util::*, exec_events::ConsensusState};

#[derive(clap::Subcommand, Clone, Debug)]
pub enum Command {
    #[command(about = "Dump CVT events to stdout")]
    Dump {
        #[arg(help = "path of the ring shared memory snapshot file")]
        actual: PathBuf,

        #[arg(short, long, help = "aggregate all updates into an array first")]
        array: bool,
    },

    #[command(about = "Run CVT test (compare execution event stream to ground truth)")]
    Compare {
        #[arg(help = "path of the expected value JSON file; may be zstd compressed")]
        expected: PathBuf,

        #[arg(help = "path of the ring shared memory snapshot file")]
        actual: PathBuf,
    },
}

#[derive(Parser)]
#[command(about = "Event cross-validation test utility")]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

fn open_event_ring_snapshot(event_ring_file_path: PathBuf) -> EventRing {
    let zstd_bytes = std::fs::read(&event_ring_file_path).expect(
        format!(
            "error in reading actual events zstd file: {:#?}",
            event_ring_file_path
        )
        .as_str(),
    );
    let actual_error_name = event_ring_file_path.to_str().unwrap();

    let actual_snap =
        EventRingSnapshot::load_from_zstd_bytes(zstd_bytes.as_slice(), actual_error_name);
    let mmap_prot = libc::PROT_READ;
    let mmap_extra_flags = 0;
    match EventRing::mmap_from_fd(
        mmap_prot,
        mmap_extra_flags,
        actual_snap.snapshot_fd,
        actual_snap.snapshot_off,
        actual_error_name,
    ) {
        Err(e) => {
            eprintln!("event library error -- {e}");
            process::exit(1);
        }
        Ok(r) => r,
    }
}

fn dump_cvt_updates(event_ring_file: PathBuf, array: bool) {
    let event_ring = open_event_ring_snapshot(event_ring_file);
    let mut cvt_stream = match CrossValidationTestStream::new(&event_ring) {
        Err(e) => {
            eprintln!("event library error -- {e}");
            process::exit(1);
        }
        Ok(s) => s,
    };
    let mut update_array = Vec::new();
    while let Some(u) = cvt_stream.poll() {
        if array {
            update_array.push(u);
        } else {
            println!("{}", serde_json::to_string_pretty(&u).unwrap());
        }
    }
    if array {
        println!("{}", serde_json::to_string_pretty(&update_array).unwrap());
    }
}

fn run_cvt_test(expected: PathBuf, actual: PathBuf) {
    let mut file_bits = std::fs::read(&expected)
        .expect(format!("unable to open file {}", expected.display()).as_str());

    const MAX_FILE_SIZE: usize = 1 << 30;
    let zstd_decomp_result = zstd::bulk::decompress(file_bits.as_slice(), MAX_FILE_SIZE);
    if zstd_decomp_result.is_ok() {
        // Successful compression means we change the file_bits to the
        // decompression result, otherwise we assume that the original
        // file was not compressed and that was the reason for failure.
        // It doesn't seem that we can be much smarter: the error type
        // returned by decompress remaps the underlying error, obscuring
        // the original libzstd code.
        file_bits = zstd_decomp_result.unwrap();
    }

    // Deserialize the expected updates from the input file
    let mut expected_block_updates: Vec<CrossValidationTestUpdate> =
        serde_json::from_slice(file_bits.as_slice()).unwrap();

    // For the actual updates, we'll open the compressed snapshot of the
    // underlying events
    let event_ring = open_event_ring_snapshot(actual);
    let mut cvt_stream = match CrossValidationTestStream::new(&event_ring) {
        Err(e) => {
            eprintln!("event library error -- {e}");
            process::exit(1);
        }
        Ok(s) => s,
    };

    let mut actual_block_updates: Vec<CrossValidationTestUpdate> = Vec::new();
    while let Some(u) = cvt_stream.poll() {
        if let CrossValidationTestUpdate::UnexpectedEventError(e) = u {
            eprintln!("unexpected event error occurred, CVT test failed: {e}");
            process::exit(1);
        }
        if let CrossValidationTestUpdate::UnknownProposal {
            block_number: _,
            block_id: _,
            consensus_state: ConsensusState::Verified,
        } = u
        {
            // C++ cannot recover the verified unknown proposals the same way we can; this is
            // not very interesting anyway
            continue;
        }
        actual_block_updates.push(u);
    }

    let mut matched_update_count: usize = 0;
    for (expected_update, actual_update) in
        expected_block_updates.iter_mut().zip(&actual_block_updates)
    {
        if *expected_update != *actual_update {
            let expected = serde_json::to_string_pretty(&*expected_update).unwrap();
            let actual = serde_json::to_string_pretty(actual_update).unwrap();
            println!("expected:\n{expected}");
            println!("actual:\n{actual}");
            eprintln!("failure after {matched_update_count} successfully matched updates");
            process::exit(1);
        }
        matched_update_count += 1;
    }

    println!("matched all {matched_update_count} block updates");
}

fn main() {
    match Cli::parse().cmd {
        Command::Dump { actual, array } => dump_cvt_updates(actual, array),
        Command::Compare { expected, actual } => run_cvt_test(expected, actual),
    }
}
