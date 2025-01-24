//! Utility for printing block update objects (from the exec_update_builder
//! module) to stdout

use std::{path::PathBuf, process, str::FromStr};

use clap::Parser;
use monad_event_ring::{
    event_reader::EventReader,
    event_ring::*,
    event_test_util::{open_event_ring_file, OpenedEventRing},
    exec_event_types_metadata::{EXEC_EVENT_DEFAULT_RING_PATH, EXEC_EVENT_DOMAIN_METADATA},
};
use monad_rpc::exec_update_builder::*;

#[derive(clap::ValueEnum, Clone, Default, Debug, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
enum DisplayFormat {
    #[default]
    Summary,
    Debug,
    Json,
}

#[derive(Parser)]
#[command(about = "Utility for watching execution block updates")]
struct Cli {
    #[arg(
        short,
        long,
        help = "name of the execution event ring shared memory file"
    )]
    #[arg(default_value = EXEC_EVENT_DEFAULT_RING_PATH)]
    event_ring_path: PathBuf,

    #[arg(
        short,
        long,
        default_value = "proposed",
        help = "consensus state to reach before reporting (proposed, voted, finalized, or verified)"
    )]
    filter: String,

    #[arg(
        short,
        long,
        help = "format used to display the updates",
        default_value = "summary"
    )]
    display: DisplayFormat,

    #[arg(short, long, help = "report orphaned consensus states")]
    orphans: bool,

    #[arg(
        short,
        long,
        help = "transaction input will not be included in block updates",
        default_value_t = false
    )]
    ignore_txn_input: bool,
}

fn print_failed_block_info(fail_info: Box<FailedBlockInfo>) {
    let number = fail_info.eth_header.number;
    let bft_block_id = fail_info.bft_block_id;
    let error = fail_info.failure_reason;
    println!("Failure   => {number} [{bft_block_id:#?}] ERROR: {error:#?}");
}

fn print_executed_block_info(exec_info: Box<ExecutedBlockInfo>) {
    let number = exec_info.eth_header.number;
    let bft_block_id = exec_info.bft_block_id;
    let txn_count = exec_info.txns.len();
    let gas = exec_info.eth_header.gas_used;
    let mut log_count: u64 = 0;
    for ref txn_info_opt in exec_info.txns {
        let txn_info = txn_info_opt.as_ref().unwrap();
        log_count += txn_info.receipt.logs.len() as u64;
    }
    println!("{number} [{bft_block_id:#?}] TXNS: {txn_count:4} GAS: {gas:8} LOGS: {log_count:4}");
}

fn print_speculative_update_summary(u: BlockUpdate) {
    match u {
        BlockUpdate::Failed(fail_info) => print_failed_block_info(fail_info),
        BlockUpdate::Executed {
            consensus_state,
            exec_info,
        } => {
            let state_name = format!("{consensus_state:?}");
            print!("{state_name:9} => ");
            print_executed_block_info(exec_info)
        }
        BlockUpdate::ConsensusStateChanged {
            new_state,
            bft_block_id,
            block_number,
            has_untracked_proposal,
        } => {
            let state_name = format!("{new_state:?}");
            let suffix = if has_untracked_proposal {
                " <orphan>"
            } else {
                ""
            };
            println!("{state_name:9} => {block_number} [{bft_block_id:#?}]{suffix}");
        }
    }
}
fn print_event_stream_error(err: &EventStreamError) {
    match err {
        EventStreamError::Disconnected => eprintln!("ERROR: server disconnected"),
        EventStreamError::Gap {
            last_seqno,
            cur_seqno,
        } => {
            eprintln!("ERROR: event gap from {last_seqno} to {cur_seqno}");
        }
        EventStreamError::PayloadExpired {
            payload_offset,
            buffer_window_start,
        } => {
            eprintln!("ERROR: event payload expiration of {payload_offset}, minimum window is {buffer_window_start}");
        }
        EventStreamError::PayloadTruncated(s) => {
            eprintln!("ERROR: payload {s} was too large");
        }
        EventStreamError::ProtocolError(s) => {
            eprintln!("ERROR: event stream protocol error {s}");
        }
    }
}

fn event_loop(mut builder: BlockUpdateBuilder, display: DisplayFormat) {
    loop {
        match builder.poll() {
            BlockPollResult::NotReady => (),
            BlockPollResult::Error(err) => {
                print_event_stream_error(&err);
                match err {
                    EventStreamError::Disconnected => process::exit(libc::EXIT_SUCCESS),
                    EventStreamError::ProtocolError(_) => process::exit(libc::EXIT_FAILURE),
                    _ => (),
                }
            }
            BlockPollResult::Ready(u) => match display {
                DisplayFormat::Summary => print_speculative_update_summary(u),
                DisplayFormat::Debug => println!("{u:#?}"),
                DisplayFormat::Json => println!("{}", serde_json::to_string_pretty(&u).unwrap()),
            },
        }
    }
}

fn main() {
    let cli = Cli::parse();

    let event_ring;
    let opt_process_exit_monitor;
    let is_snapshot;
    let executed_consensus_state = match BlockConsensusState::from_str(cli.filter.as_str()) {
        Err(()) => {
            eprintln!(
                "unable to parse `{}` as a block consensus state",
                cli.filter
            );
            process::exit(libc::EXIT_FAILURE);
        }
        Ok(r) => r,
    };

    match open_event_ring_file(&cli.event_ring_path) {
        Err(e) => {
            eprintln!("{e}");
            process::exit(libc::EXIT_FAILURE);
        }
        Ok(o) => match o {
            OpenedEventRing::Live(r, p) => {
                event_ring = r;
                opt_process_exit_monitor = Some(p);
                is_snapshot = false;
            }
            OpenedEventRing::Snapshot(r) => {
                event_ring = r;
                opt_process_exit_monitor = None;
                is_snapshot = true;
            }
        },
    }

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

    if is_snapshot {
        event_reader.read_last_seqno = 0;
    }

    event_loop(
        BlockUpdateBuilder::new(
            &event_ring,
            event_reader,
            BlockUpdateBuilderConfig {
                executed_consensus_state,
                parse_txn_input: !cli.ignore_txn_input,
                report_orphaned_consensus_events: cli.orphans,
                opt_process_exit_monitor,
            },
        ),
        cli.display,
    );
}
