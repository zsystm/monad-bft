use std::{path::PathBuf, time::Duration};

use clap::Parser;
use monad_event_ring::{EventNextResult, EventRing, TypedEventReader, TypedEventRing};
use monad_exec_events::{
    BlockBuilderResult, BlockCommitState, ConsensusStateTracker, ConsensusStateTrackerUpdate,
    ExecEventRingType,
};

#[derive(Debug, Parser)]
#[command(name = "monad-exec-events-blockdump-filtered", about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    event_ring_path: PathBuf,

    #[arg(long)]
    filter_commit_state: Option<String>,
}

fn main() {
    let Cli {
        event_ring_path,
        filter_commit_state,
    } = Cli::parse();

    let filter_commit_state = filter_commit_state.map(|input| match input.as_str() {
        "proposed" => BlockCommitState::Proposed,
        "voted" => BlockCommitState::Voted,
        "finalized" => BlockCommitState::Finalized,
        "verified" => BlockCommitState::Verified,
        _ => panic!("unknown commit state"),
    });

    let event_ring = EventRing::<ExecEventRingType>::new_from_path(event_ring_path).unwrap();

    let mut event_reader = event_ring.create_reader();

    let mut state_tracker = ConsensusStateTracker::default();

    loop {
        let event = match event_reader.next() {
            EventNextResult::Gap => panic!("event ring gapped"),
            EventNextResult::NotReady => {
                std::thread::sleep(Duration::from_millis(1));
                continue;
            }
            EventNextResult::Ready(event) => event,
        };

        let Some(result) = state_tracker.process_event_descriptor(&event) else {
            continue;
        };

        match result {
            BlockBuilderResult::Rejected => panic!("execution rejected block"),
            BlockBuilderResult::PayloadExpired => panic!("event ring payload expired"),
            BlockBuilderResult::ImplicitDrop { .. } => unreachable!(),
            BlockBuilderResult::Ok(ConsensusStateTrackerUpdate {
                block,
                state,
                abandoned: _,
            }) => {
                if let Some(filter_commit_state) = filter_commit_state {
                    if filter_commit_state != state {
                        continue;
                    }
                }

                println!("{:?}", block);
            }
        }
    }
}
