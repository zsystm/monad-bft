use std::{path::PathBuf, time::Duration};

use clap::Parser;
use monad_event_ring::{EventNextResult, EventRing, TypedEventReader, TypedEventRing};
use monad_exec_events::{BlockBuilder, BlockBuilderResult, ExecEventRingType};

#[derive(Debug, Parser)]
#[command(name = "monad-exec-events-blockdump", about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    event_ring_path: PathBuf,
}

fn main() {
    let Cli { event_ring_path } = Cli::parse();

    let event_ring = EventRing::<ExecEventRingType>::new_from_path(event_ring_path).unwrap();

    let mut event_reader = event_ring.create_reader();

    let mut block_builder = BlockBuilder::default();

    loop {
        let event = match event_reader.next() {
            EventNextResult::Gap => panic!("event ring gapped"),
            EventNextResult::NotReady => {
                std::thread::sleep(Duration::from_millis(1));
                continue;
            }
            EventNextResult::Ready(event) => event,
        };

        let Some(result) = block_builder.process_event_descriptor(&event) else {
            continue;
        };

        match result {
            BlockBuilderResult::Rejected => unreachable!(),
            BlockBuilderResult::PayloadExpired => panic!("event ring payload expired"),
            BlockBuilderResult::ImplicitDrop { .. } => unreachable!(),
            BlockBuilderResult::Ok(executed_block) => {
                println!("{executed_block:?}");
            }
        }
    }
}
