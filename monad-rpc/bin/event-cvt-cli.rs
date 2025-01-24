//! A command line interface to run instances of the execution event
//! "cross-validation test" (CVT)
//!
//! A #[test] within exec_update_builder.rs performs a specific instance of
//! the CVT test using fixed input data; this command line utility allows
//! you to re-run a CVT test with arbitrary inputs.
//!
//! The cross-validation test is defined in the following way:
//!
//! - The execution daemon can export a JSON view of what it thinks a Rust
//!   BlockUpdate should look like. Because all the major structures in the
//!   exec_update_builder module use #[derive(Deserialize)] from the serde
//!   package, Rust can easily load the serialized C++ "ground truth."
//!
//! - This utility starts by loading a persisted snapshot of the event ring
//!   file that has been saved to disk, using the event ring's "test utility"
//!   module. This snapshot should contain the same low-level event stream for
//!   which C++ exported the "ground truth" JSON for BlockUpdate objects.
//!
//! - This utility then uses a BlockUpdateBuilder object to reassemble the
//!   persisted event stream into its own view of the block updates.
//!
//! - As a final step, it zips the two Vec<BlockUpdate> inputs together and
//!   checks that each pair matches exactly
//!
//! The idea behind the cross-validation test is that we're computing the
//! same thing in two very different ways, one of which is very circuitous
//! (execution recorder -> shared memory -> Rust event library -> Rust
//! reassembly library), the other of which is direct and does not use any
//! of the same code.

use std::{path::PathBuf, process};

use clap::Parser;
use monad_event_ring::{
    event_reader::EventReader, event_ring::*, event_test_util::EventRingSnapshot,
    exec_event_types_metadata::EXEC_EVENT_DOMAIN_METADATA,
};
use monad_rpc::exec_update_builder::*;

#[derive(Parser)]
#[command(about = "Event cross-validation test utility")]
struct Cli {
    #[arg(help = "path of the expected value JSON file; may be zstd compressed")]
    expected: PathBuf,

    #[arg(help = "path of the ring shared memory snapshot file")]
    actual: PathBuf,
}

fn main() {
    const MAX_FILE_SIZE: usize = 1 << 30;
    let cli = Cli::parse();

    let mut file_bits = std::fs::read(&cli.expected)
        .expect(format!("unable to open file {}", cli.expected.display()).as_str());

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
    let mut expected_block_updates: Vec<BlockUpdate> =
        serde_json::from_slice(file_bits.as_slice()).unwrap();

    // For the actual updates, we'll open the compressed snapshot of the
    // underlying events
    let zstd_bytes = std::fs::read(&cli.actual).expect(
        format!(
            "error in reading actual events zstd file: {:#?}",
            cli.actual
        )
        .as_str(),
    );
    let actual_error_name = cli.actual.to_str().unwrap();
    let actual_snap =
        EventRingSnapshot::load_from_zstd_bytes(zstd_bytes.as_slice(), actual_error_name);

    let mmap_prot = libc::PROT_READ;
    let mmap_extra_flags = 0;
    let event_ring = match EventRing::mmap_from_fd(
        mmap_prot,
        mmap_extra_flags,
        actual_snap.snapshot_fd,
        actual_snap.snapshot_off,
        actual_error_name,
    ) {
        Err(e) => {
            eprintln!("event library error -- {e}");
            process::exit(libc::EXIT_FAILURE);
        }
        Ok(r) => r,
    };
    let mut event_reader = match EventReader::new(
        &event_ring,
        EventRingType::Exec,
        &EXEC_EVENT_DOMAIN_METADATA.metadata_hash,
    ) {
        Err(e) => {
            eprintln!("event library error -- {e}");
            process::exit(libc::EXIT_FAILURE);
        }
        Ok(r) => r,
    };
    event_reader.read_last_seqno = 0;
    let mut update_builder = BlockUpdateBuilder::new(
        &event_ring,
        event_reader,
        BlockUpdateBuilderConfig {
            executed_consensus_state: BlockConsensusState::Proposed,
            parse_txn_input: true,
            report_orphaned_consensus_events: true,
            opt_process_exit_monitor: None,
        },
    );

    let mut actual_block_updates: Vec<BlockUpdate> = Vec::new();
    loop {
        match update_builder.poll() {
            BlockPollResult::Ready(update) => actual_block_updates.push(update),
            BlockPollResult::NotReady => break,
            _ => unreachable!(),
        }
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
