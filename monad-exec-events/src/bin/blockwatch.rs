//! Utility for printing block update objects to stdout

use std::{path::PathBuf, process, str::FromStr};

use clap::Parser;
use monad_event_ring::{
    event_reader::EventReader,
    event_ring::*,
    event_ring_util::{open_event_ring_file, OpenedEventRing},
};
use monad_exec_events::{
    block_builder::*,
    consensus_state_tracker::*,
    exec_event_ctypes::{EXEC_EVENT_DEFAULT_RING_PATH, EXEC_EVENT_DOMAIN_METADATA},
    exec_event_stream::*,
    exec_events::{ConsensusState, ExecEvent, ProposalMetadata},
};

#[derive(clap::ValueEnum, Copy, Clone, Default, Debug, serde::Serialize)]
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

    #[arg(
        short,
        long,
        help = "dump the full execution information for abandoned proposals, if not dumped already"
    )]
    abandoned_details: bool,

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

fn print_failed_block_info(fail_info: Box<FailedBlockInfo>, display: DisplayFormat) {
    match display {
        DisplayFormat::Summary => {
            const FAILURE: &str = "Failure";
            let number = fail_info.eth_header.number;
            let block_id = fail_info.proposal_meta.id.0;
            let error = fail_info.failure_reason;
            println!("{FAILURE:9} => {number} [{block_id:#?}] ERROR: {error:#?}");
        }
        DisplayFormat::Debug => println!("{fail_info:#?}"),
        DisplayFormat::Json => println!("{}", serde_json::to_string_pretty(&fail_info).unwrap()),
    }
}

fn print_executed_block_info(
    exec_info: &ExecutedBlockInfo,
    consensus_state: Option<ConsensusState>,
    display: DisplayFormat,
) {
    match display {
        DisplayFormat::Summary => {
            let number = exec_info.eth_header.number;
            let block_id = exec_info.proposal_meta.id.0;
            let txn_count = exec_info.transactions.len();
            let gas = exec_info.eth_header.gas_used;
            let mut log_count: u64 = 0;
            for txn_info_opt in &exec_info.transactions {
                let txn_info = txn_info_opt.as_ref().unwrap();
                log_count += txn_info.receipt.logs.len() as u64;
            }
            let consensus_state = match consensus_state {
                Some(s) => format!("{s:?}"),
                None => String::new(),
            };
            println!("{consensus_state:9} => {number} [{block_id:#?}] TXNS: {txn_count:4} GAS: {gas:8} LOGS: {log_count:4}");
        }
        DisplayFormat::Debug => println!("{exec_info:#?}"),
        DisplayFormat::Json => println!("{}", serde_json::to_string_pretty(&exec_info).unwrap()),
    }
}

fn print_referendum_update(
    proposal_meta: ProposalMetadata,
    outcome: ConsensusState,
    display: DisplayFormat,
    is_orphan: bool,
) {
    match display {
        DisplayFormat::Summary => {
            let block_number = proposal_meta.block_number;
            let block_id = proposal_meta.id.0;
            let state = format!("{outcome:?}");
            let suffix = if is_orphan { " <orphan>" } else { "" };
            println!("{state:9} => {block_number} [{block_id:#?}]{suffix}")
        }
        DisplayFormat::Debug => println!(
            "{}{:#?}",
            if is_orphan { "ORPHAN: " } else { "" },
            ExecEvent::Referendum {
                proposal_meta,
                outcome
            }
        ),
        DisplayFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&ExecEvent::Referendum {
                proposal_meta,
                outcome
            })
            .unwrap()
        ),
    }
}

fn print_abandoned_proposals(
    abandoned_proposals: Vec<std::sync::Arc<ProposalState<Box<ExecutedBlockInfo>>>>,
    earliest_consensus_state: ConsensusState,
    display: DisplayFormat,
    show_abandoned_details: bool,
) {
    for abandoned_block in abandoned_proposals {
        const ABANDONED: &str = "Abandoned";
        let abandoned_block = abandoned_block;
        let number = abandoned_block.proposal_meta.block_number;
        let block_id = abandoned_block.proposal_meta.id;
        println!("{ABANDONED:9} => {number} [{block_id:#?}]");
        let abandoned_state = *abandoned_block.current_state.read().unwrap();
        if abandoned_state.lt(&earliest_consensus_state) && show_abandoned_details {
            print_executed_block_info(&abandoned_block.user_data, None, display);
        }
    }
}

fn event_loop(
    mut event_stream: ExecEventStream,
    earliest_consensus_state: ConsensusState,
    display: DisplayFormat,
    show_orphans: bool,
) {
    use std::cmp::Ordering;
    let mut block_builder = BlockBuilder::new();
    let mut consensus_tracker: ConsensusStateTracker<Box<ExecutedBlockInfo>> =
        ConsensusStateTracker::new();

    loop {
        let exec_event = match event_stream.poll() {
            PollResult::NotReady => continue,
            PollResult::Disconnected => process::exit(0),
            err @ PollResult::Gap { .. } | err @ PollResult::PayloadExpired { .. } => {
                eprintln!("ERROR: {err:?}");
                let _ = block_builder.drop_block();
                consensus_tracker.reset_all();
                continue;
            }
            PollResult::Ready { seqno: _, event } => event,
        };

        match block_builder.try_append(exec_event) {
            Some(BlockUpdate::Failed(fail_info)) => print_failed_block_info(fail_info, display),

            Some(BlockUpdate::Executed(exec_info)) => {
                if exec_info.consensus_state == earliest_consensus_state {
                    print_executed_block_info(
                        exec_info.as_ref(),
                        Some(exec_info.consensus_state),
                        display,
                    )
                }
                if let ConsensusStateResult::Finalization {
                    finalized_proposal: _,
                    abandoned_proposals,
                } = consensus_tracker.add_proposal(
                    exec_info.proposal_meta,
                    exec_info.consensus_state,
                    exec_info,
                ) {
                    print_abandoned_proposals(
                        abandoned_proposals,
                        earliest_consensus_state,
                        display,
                        show_orphans,
                    );
                }
            }

            Some(BlockUpdate::NonBlockEvent(ExecEvent::Referendum {
                proposal_meta,
                outcome,
            })) => {
                // A consensus referendum on a proposal has concluded; look up the associated
                // proposal
                let block_number = proposal_meta.block_number;
                let block_id = &proposal_meta.id;
                if let Some(pending_block) = consensus_tracker.get(block_number, block_id) {
                    // If the outcome of the referendum puts the proposal in an
                    // earlier state than `earliest_consensus_state` (e.g., if
                    // the outcome moves a proposal to `QC` but the earliest we
                    // print to stdout is `Finalized`) then do nothing. If we've
                    // reached the earliest consensus state that we print, also
                    // print the execution information. If this is a referendum
                    // for a proposal we have already printed, then print a
                    // "referendum update" line.
                    match outcome.cmp(&earliest_consensus_state) {
                        Ordering::Less => (),
                        Ordering::Equal => print_executed_block_info(
                            &pending_block.user_data,
                            Some(outcome),
                            display,
                        ),
                        Ordering::Greater => {
                            print_referendum_update(proposal_meta, outcome, display, false);
                        }
                    }
                } else if show_orphans {
                    // There was no such proposal; this can happen if we start listening to
                    // events after the execution daemon is already running. We'll see
                    // events (e.g., a finalization referendum) for proposals that we never
                    // saw execute. Because `show_orphans` is also set, we print a "referendum
                    // update" line with an ORPHAN prefix.
                    print_referendum_update(proposal_meta, outcome, display, true);
                }
                if let ConsensusStateResult::Finalization {
                    finalized_proposal: _,
                    abandoned_proposals,
                } = consensus_tracker.update_proposal(block_number, block_id, outcome)
                {
                    print_abandoned_proposals(
                        abandoned_proposals,
                        earliest_consensus_state,
                        display,
                        show_orphans,
                    );
                }
            }

            Some(BlockUpdate::NonBlockEvent(_)) | Some(BlockUpdate::OrphanedEvent(_)) | None => (),

            Some(BlockUpdate::ImplicitDrop {
                dropped_block,
                reassembly_error,
            }) => {
                panic!("ImplicitDrop but we called drop_block ourselves: {dropped_block:#?} {reassembly_error:#?}");
            }
        }
    }
}

fn main() {
    let cli = Cli::parse();

    let event_ring;
    let opt_process_exit_monitor;
    let is_snapshot;
    let earliest_consensus_state = match ConsensusState::from_str(cli.filter.as_str()) {
        Err(()) => {
            eprintln!(
                "unable to parse `{}` as a block consensus state",
                cli.filter
            );
            process::exit(1);
        }
        Ok(r) => r,
    };

    match open_event_ring_file(&cli.event_ring_path) {
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
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
            process::exit(1);
        }
        Ok(r) => r,
    };

    if is_snapshot {
        event_reader.read_last_seqno = 0;
    }

    let config = ExecEventStreamConfig {
        parse_txn_input: !cli.ignore_txn_input,
        opt_process_exit_monitor,
    };

    event_loop(
        ExecEventStream::new(event_reader, config),
        earliest_consensus_state,
        cli.display,
        cli.orphans,
    );
}
