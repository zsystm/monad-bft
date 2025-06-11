use monad_event_ring::{
    EventRingType, SnapshotEventReader, SnapshotEventRing, TypedEventReader, TypedEventRing,
};
use monad_exec_events::{
    BlockBuilder, BlockBuilderResult, BlockCommitState, ExecEventRingType, ExecEvents,
};

fn main() {
    let snapshot = SnapshotEventRing::<ExecEventRingType>::new_from_zstd_bytes(
        include_bytes!("../../test/data/exec-events-emn-30b-15m.zst"),
        "ETHEREUM_MAINNET_30B_15M",
    )
    .unwrap();

    let mut event_reader: monad_event_ring::SnapshotEventReader<'_, ExecEventRingType> =
        snapshot.create_reader();

    event_loop(&mut event_reader, BlockCommitState::Finalized);
}

// NOTE: what is the point of TypedEventReader if you can't abstract over various concrete impls?
// fn event_loop<'ring>(
//     event_reader: &mut (impl TypedEventReader<'ring, Event<'ring> = ExecEvents> + 'ring),
//     earliest_commit_state: BlockCommitState,
// ) {

fn event_loop<'ring>(
    event_reader: &mut SnapshotEventReader<'ring, ExecEventRingType>,
    earliest_commit_state: BlockCommitState,
) {
    let mut block_builder = BlockBuilder::default();

    while let Some(event_descriptor) = event_reader.next() {
        let Some(result) = block_builder.process_snapshot_event_descriptor(&event_descriptor)
        else {
            continue;
        };

        match result {
            BlockBuilderResult::Failed() => {
                println!("failed :(")
            }
            BlockBuilderResult::PayloadExpired => {
                println!("payload expired");
            }
            BlockBuilderResult::ImplicitDrop {
                block,
                reassembly_error,
            } => {
                panic!("ImplicitDrop but we called drop_block ourselves: {block:#?} {reassembly_error:#?}");
            }
            BlockBuilderResult::ExecutedBlock(executed_block) => {
                //
            }
        }
    }
}

fn print_failed_block_info() {}
