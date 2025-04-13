//! A command line interface to run instances of the execution event
//! "cross-validation test" (CVT)
//!
//! A #[test] within exec_event_test_util.rs performs a specific instance of
//! the CVT test using fixed input data; this command line utility allows
//! you to re-run a CVT test with arbitrary inputs.

use std::{io::Read, path::PathBuf, process};

use clap::Parser;
use monad_event_ring::{event_ring::EventRing, event_ring_util::EventRingSnapshot};
use monad_exec_events::{exec_event_test_util::*, exec_events::ConsensusState};

#[derive(clap::Subcommand, Clone, Debug)]
pub enum Command {
    #[command(about = "Dump CVT updates to stdout")]
    Dump {
        #[arg(help = "path to the expected value JSON or ring shared memory snapshot file")]
        input_file: PathBuf,

        #[arg(short, long, help = "aggregate all updates into an array first")]
        array: bool,
    },

    #[command(about = "Run CVT test (compare execution event stream to ground truth)")]
    Compare {
        #[arg(
            help = "path of the expected value JSON file or ring shared shared memory snapshot file"
        )]
        input_files: Vec<PathBuf>,
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

    let actual_snap = EventRingSnapshot::load_from_bytes(zstd_bytes.as_slice(), actual_error_name);
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

enum CvtInput {
    Expected(Vec<CrossValidationTestUpdate>),
    Actual(EventRing),
}

fn open_cvt_input_file(file_name: PathBuf) -> CvtInput {
    let error_name = file_name.to_str().unwrap();
    let mut input_file = match std::fs::File::open(&file_name) {
        Err(e) => {
            eprintln!("open of CVT input file `{error_name}` failed: {e}");
            process::exit(1);
        }
        Ok(f) => f,
    };

    use std::os::unix::fs::FileExt;
    let mut magic_buf = [0; 4];
    if let Err(e) = input_file.read_exact_at(magic_buf.as_mut_slice(), 0) {
        eprintln!("failed to read magic number from event ring file `{error_name}`: {e}");
        process::exit(1);
    }

    let mut file_bits: Vec<u8> = Vec::new();
    if u32::from_le_bytes(magic_buf) == zstd::zstd_safe::MAGICNUMBER {
        let _ = input_file
            .read_to_end(&mut file_bits)
            .expect(format!("unable to read file {error_name}").as_str());
        const MAX_FILE_SIZE: usize = 1 << 30;
        file_bits = match zstd::bulk::decompress(file_bits.as_slice(), MAX_FILE_SIZE) {
            Err(e) => {
                eprintln!("zstd decompression of {error_name} failed: {e}");
                process::exit(1);
            }
            Ok(v) => v,
        };
    } else {
        let _ = input_file
            .read_to_end(&mut file_bits)
            .expect(format!("unable to read file {}", file_name.display()).as_str());
    }

    const RING_MAGIC_4: [u8; 4] = [b'R', b'I', b'N', b'G'];
    if file_bits.as_slice()[..4] == RING_MAGIC_4 {
        let actual_snap = EventRingSnapshot::load_from_bytes(file_bits.as_slice(), error_name);
        let mmap_prot = libc::PROT_READ;
        let mmap_extra_flags = 0;
        match EventRing::mmap_from_fd(
            mmap_prot,
            mmap_extra_flags,
            actual_snap.snapshot_fd,
            actual_snap.snapshot_off,
            error_name,
        ) {
            Err(e) => {
                eprintln!("event library error -- {e}");
                process::exit(1);
            }
            Ok(r) => CvtInput::Actual(r),
        }
    } else {
        CvtInput::Expected(serde_json::from_slice(file_bits.as_slice()).unwrap())
    }
}

fn dump_cvt_updates(file_path: PathBuf, array: bool) {
    let cvt_updates = match open_cvt_input_file(file_path) {
        CvtInput::Expected(updates) => updates,

        CvtInput::Actual(event_ring) => {
            let mut cvt_stream = match CrossValidationTestStream::new(&event_ring) {
                Err(e) => {
                    eprintln!("event library error -- {e}");
                    process::exit(1);
                }
                Ok(s) => s,
            };
            let mut update_array = Vec::new();
            while let Some(u) = cvt_stream.poll() {
                update_array.push(u);
            }
            update_array
        }
    };

    if array {
        println!("{}", serde_json::to_string_pretty(&cvt_updates).unwrap());
    } else {
        for update in cvt_updates {
            println!("{}", serde_json::to_string_pretty(&update).unwrap());
        }
    }
}

fn run_cvt_test(input_files: Vec<PathBuf>) {
    // TODO(ken): how to get clap to do this automatically?
    if input_files.len() != 2 {
        eprintln!(
            "CVT test requires exactly 2 file arguments but found {}",
            input_files.len()
        );
        process::exit(1);
    }

    // Deserialize the expected updates from the input file
    let mut expected_block_updates: Option<Vec<CrossValidationTestUpdate>> = None;
    let mut event_ring: Option<EventRing> = None;

    for input_file in input_files {
        match open_cvt_input_file(input_file) {
            CvtInput::Expected(ex) => {
                expected_block_updates.replace(ex);
            }
            CvtInput::Actual(r) => {
                event_ring.replace(r);
            }
        }
    }

    if expected_block_updates.is_none() || event_ring.is_none() {
        eprintln!(
            "expected one expected JSON file and one event ring file, but both have the same type"
        );
        process::exit(1);
    }

    let expected_block_updates = expected_block_updates.unwrap();
    let event_ring = event_ring.unwrap();

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
    for (expected_update, actual_update) in expected_block_updates.iter().zip(&actual_block_updates)
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
        Command::Dump { input_file, array } => dump_cvt_updates(input_file, array),
        Command::Compare { input_files } => run_cvt_test(input_files),
    }
}
