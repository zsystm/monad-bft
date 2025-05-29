use std::{
    collections::{hash_map::Entry as HashMapEntry, BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use monad_event_ring::{EventDescriptor, EventDescriptorPayload, SnapshotEventDescriptor};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    block_builder::{BlockBuilder, BlockBuilderResult, ExecutedBlock, ReassemblyError},
    ffi::{monad_c_bytes32, monad_exec_block_header},
    ExecEventRingType, ExecEvents, ExecEventsRef,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockCommitState {
    Proposed,
    Voted,
    Finalized,
    Verified,
}

#[derive(Debug)]
pub enum ConsensusStateTrackerResult {
    PayloadExpired,

    ImplicitDrop {
        block: monad_exec_block_header,
        reassembly_error: ReassemblyError,
    },

    Update {
        block: Arc<ExecutedBlock>,
        state: BlockCommitState,
        abandoned: Option<Vec<Arc<ExecutedBlock>>>,
    },
}

#[derive(Default)]
pub struct ConsensusStateTracker {
    state: HashMap<monad_c_bytes32, (Arc<ExecutedBlock>, BlockCommitState)>,
    proposals: BTreeMap<u64, HashSet<monad_c_bytes32>>,

    block_builder: BlockBuilder,
}

impl std::fmt::Debug for ConsensusStateTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusStateTracker")
            .field(
                "state",
                &self
                    .state
                    .iter()
                    .map(|(id, (_, state))| (id.bytes[0], state))
                    .collect::<Vec<_>>(),
            )
            .field(
                "proposals",
                &self
                    .proposals
                    .iter()
                    .map(|(block, ids)| {
                        (
                            block,
                            ids.into_iter().map(|id| id.bytes[0]).collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl ConsensusStateTracker {
    pub fn process_event_descriptor<'ring, 'reader>(
        &mut self,
        event_descriptor: &EventDescriptor<'ring, 'reader, ExecEventRingType>,
    ) -> Option<ConsensusStateTrackerResult> {
        match event_descriptor.try_map_payload(Self::select_event_ref) {
            EventDescriptorPayload::Payload(Some(exec_event)) => {
                return self.process_exec_event(exec_event);
            }
            EventDescriptorPayload::Payload(None) => {}
            EventDescriptorPayload::Expired => {
                self.reset();

                return Some(ConsensusStateTrackerResult::PayloadExpired);
            }
        }

        self.block_builder
            .process_event_descriptor(event_descriptor)
            .map(|result| self.process_block_builder_result(result))
    }

    pub fn process_snapshot_event_descriptor<'ring, 'reader>(
        &mut self,
        event_descriptor: &SnapshotEventDescriptor<'ring, 'reader, ExecEventRingType>,
    ) -> Option<ConsensusStateTrackerResult> {
        if let Some(exec_event) = event_descriptor.try_map_payload(Self::select_event_ref) {
            return self.process_exec_event(exec_event);
        }

        self.block_builder
            .process_snapshot_event_descriptor(event_descriptor)
            .map(|result| self.process_block_builder_result(result))
    }

    fn select_event_ref(event_ref: ExecEventsRef<'_>) -> Option<ExecEvents> {
        match event_ref {
            event @ (ExecEventsRef::BlockQC(_)
            | ExecEventsRef::BlockFinalized(_)
            | ExecEventsRef::BlockVerified(_)) => Some(event.into_owned()),
            _ => None,
        }
    }

    fn process_exec_event(
        &mut self,
        exec_event: ExecEvents,
    ) -> Option<ConsensusStateTrackerResult> {
        match exec_event {
            ExecEvents::BlockQC(proposal_metadata) => {
                let Some((executed_block, state)) = self.state.get_mut(&proposal_metadata.id)
                else {
                    assert!(
                        self.proposals
                            .get(&proposal_metadata.block_number)
                            .map_or(true, |proposals| !proposals.contains(&proposal_metadata.id)),
                        "ConsensusStateTracker received qc for block in proposals but not in state"
                    );
                    return None;
                };

                if state != &BlockCommitState::Proposed {
                    warn!(?state, "ConsensusStateTracker received voted block number for non-proposed proposal");
                }
                *state = BlockCommitState::Voted;

                let (block, state) = (executed_block.clone(), *state);

                let proposals = self
                    .proposals
                    .get_mut(&proposal_metadata.block_number)
                    .expect("ConsensusStateTracker added qc block to state but not proposals");
                assert!(
                    proposals.contains(&proposal_metadata.id),
                    "ConsensusStateTracker received qc for block in state but not in proposals"
                );

                let abandoned = proposals
                    .drain()
                    .filter(|block_id| block_id != &proposal_metadata.id)
                    .map(|abandoned| {
                        self.state
                            .remove(&abandoned)
                            .expect("ConsensusStateTracker added block to proposals but not blocks")
                            .0
                    })
                    .collect::<Vec<_>>();

                proposals.insert(proposal_metadata.id);

                Some(ConsensusStateTrackerResult::Update {
                    block,
                    state,
                    abandoned: if abandoned.is_empty() {
                        None
                    } else {
                        Some(abandoned)
                    },
                })
            }
            ExecEvents::BlockFinalized(proposal_metadata) => {
                let Some((executed_block, state)) = self.state.get_mut(&proposal_metadata.id)
                else {
                    assert!(
                        self.proposals
                            .get(&proposal_metadata.block_number)
                            .map_or(true, |proposals| !proposals.contains(&proposal_metadata.id)),
                        "ConsensusStateTracker received finalized for block in proposals but not in state"
                    );
                    return None;
                };

                if state != &BlockCommitState::Voted {
                    warn!(?state, "ConsensusStateTracker received finalized block number for non-voted proposal");
                }
                *state = BlockCommitState::Finalized;

                let (block, state) = (executed_block.clone(), *state);

                let proposals = self
                    .proposals
                    .get_mut(&proposal_metadata.block_number)
                    .expect(
                        "ConsensusStateTracker added finalized block to state but not proposals",
                    );
                assert!(proposals.contains(&proposal_metadata.id),
                 "ConsensusStateTracker received finalized for block in state but not in proposals");

                let abandoned = proposals
                    .drain()
                    .filter(|block_id| block_id != &proposal_metadata.id)
                    .map(|abandoned| {
                        self.state
                            .remove(&abandoned)
                            .expect("ConsensusStateTracker added block to proposals but not blocks")
                            .0
                    })
                    .collect::<Vec<_>>();

                proposals.insert(proposal_metadata.id);

                Some(ConsensusStateTrackerResult::Update {
                    block,
                    state,
                    abandoned: if abandoned.is_empty() {
                        None
                    } else {
                        Some(abandoned)
                    },
                })
            }
            ExecEvents::BlockVerified(block_verified) => {
                let Some(proposals) = self.proposals.remove(&block_verified.block_number) else {
                    return None;
                };

                if let Some((lowest_block_number, _)) = self.proposals.first_key_value() {
                    assert!(
                        lowest_block_number > &block_verified.block_number,
                        "ConsensusStateTracker received gapped verified block number"
                    );
                }

                let mut proposals = proposals.into_iter();

                let proposal = proposals
                    .next()
                    .expect("ConsensusStateTracker proposals empty for verified block number");

                assert!(
                    proposals.next().is_none(),
                    "ConsensusStateTracker has multiple proposals for verified block number"
                );

                let (block, state) = self
                    .state
                    .remove(&proposal)
                    .expect("ConsensusStateTracker added proposal to proposals but not state");

                if state != BlockCommitState::Finalized {
                    warn!(?state, "ConsensusStateTracker received verified block number for non-finalized proposal");
                }
                // assert_eq!(state, BlockCommitState::Finalized, "ConsensusStateTracker received verified block number for non-finalized proposal");

                Some(ConsensusStateTrackerResult::Update {
                    block,
                    state: BlockCommitState::Verified,
                    abandoned: None,
                })
            }
            _ => unreachable!(),
        }
    }

    fn process_block_builder_result(
        &mut self,
        result: BlockBuilderResult,
    ) -> ConsensusStateTrackerResult {
        match result {
            BlockBuilderResult::Failed() => todo!(),
            BlockBuilderResult::PayloadExpired => ConsensusStateTrackerResult::PayloadExpired,
            BlockBuilderResult::ImplicitDrop {
                block,
                reassembly_error,
            } => ConsensusStateTrackerResult::ImplicitDrop {
                block,
                reassembly_error,
            },
            BlockBuilderResult::ExecutedBlock(executed_block) => {
                let block_id = executed_block.header.proposal.id;

                match self.state.entry(block_id.clone()) {
                    HashMapEntry::Occupied(occupied_entry) => {
                        panic!(
                            "BlockBuilder produced executed block {:x?} twice",
                            occupied_entry.key()
                        );
                    }
                    HashMapEntry::Vacant(v) => {
                        let executed_block = Arc::new(executed_block);

                        v.insert((executed_block.clone(), BlockCommitState::Proposed));

                        if !self
                            .proposals
                            .entry(executed_block.header.proposal.block_number)
                            .or_default()
                            .insert(block_id)
                        {
                            panic!("ConsensusStateTracker received block already in proposals");
                        }

                        ConsensusStateTrackerResult::Update {
                            block: executed_block,
                            state: BlockCommitState::Proposed,
                            abandoned: None,
                        }
                    }
                }
            }
        }
    }

    pub fn reset(&mut self) {
        self.state.clear();
        self.proposals.clear();

        self.block_builder.reset();
    }
}

#[cfg(test)]
mod test {
    use monad_event_ring::{SnapshotEventRing, TypedEventReader, TypedEventRing};

    use super::ConsensusStateTracker;
    use crate::{ConsensusStateTrackerResult, ExecEventRingType};

    fn run_consensus_state_tracker(
        snapshot_name: &'static str,
        snapshot_zstd_bytes: &'static [u8],
    ) {
        let snapshot = SnapshotEventRing::<ExecEventRingType>::new_from_zstd_bytes(
            snapshot_zstd_bytes,
            snapshot_name,
        )
        .unwrap();

        let mut event_reader = snapshot.create_reader();

        let mut consensus_state_tracker = ConsensusStateTracker::default();

        while let Some(event_descriptor) = event_reader.next() {
            let Some(result) =
                consensus_state_tracker.process_snapshot_event_descriptor(&event_descriptor)
            else {
                continue;
            };

            match result {
                ConsensusStateTrackerResult::PayloadExpired => panic!("payload expired"),
                ConsensusStateTrackerResult::ImplicitDrop {
                    block,
                    reassembly_error,
                } => panic!("implicit drop: {reassembly_error:?}"),
                ConsensusStateTrackerResult::Update {
                    block,
                    state,
                    abandoned,
                } => {
                    eprintln!("[{}] -> {:?}", block.header.proposal.block_number, state);

                    if let Some(abandoned) = abandoned {
                        panic!("abandoned: {abandoned:#?}");
                    }
                }
            }

            eprintln!("{consensus_state_tracker:#?}");
        }
    }

    #[test]
    fn basic_test_ethereum_mainnet() {
        const SNAPSHOT_NAME: &str = "ETHEREUM_MAINNET_30B_15M";
        const SNAPSHOT_ZSTD_BYTES: &[u8] =
            include_bytes!("../test/data/exec-events-emn-30b-15m.zst");

        run_consensus_state_tracker(&SNAPSHOT_NAME, &SNAPSHOT_ZSTD_BYTES);
    }

    #[test]
    fn basic_test_monad_testnet() {
        const SNAPSHOT_NAME: &str = "MONAD_DEVNET_21B_GENESIS";
        const SNAPSHOT_ZSTD_BYTES: &[u8] =
            include_bytes!("../test/data/exec-events-mdn-21b-genesis.zst");

        run_consensus_state_tracker(&SNAPSHOT_NAME, &SNAPSHOT_ZSTD_BYTES);
    }
}
