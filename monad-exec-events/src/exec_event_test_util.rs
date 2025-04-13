//! This module defines test utilities and reference data for use in
//! this packages test, as well as downstream packages that use
//! execution events.

use monad_event_ring::{
    event_reader::EventReader,
    event_ring::{EventRing, EventRingType},
};

use crate::{
    block_builder::{BlockBuilder, BlockUpdate, ExecutedBlockInfo},
    consensus_state_tracker::{ConsensusStateResult, ConsensusStateTracker},
    exec_event_ctypes::EXEC_EVENT_DOMAIN_METADATA,
    exec_event_stream::{ExecEventStream, ExecEventStreamConfig, PollResult},
    exec_events::{ConsensusState, ExecEvent, MonadBlockId, ProposalMetadata},
};

/// Each instance of this object references a collection of binary data
/// that represents a particular test case scenario for the execution event
/// system
pub struct ExecEventTestScenario<'a> {
    /// Human-readable name of the scenario
    pub name: &'a str,

    /// A compressed snapshot of an event ring file, compressed with zstd; we
    /// can decompress this to mock up the shared memory contents as they
    /// appeared during a real run of the execution daemon (from which this
    /// snapshot was taken)
    pub event_ring_snapshot_zst: &'a [u8],

    /// The expected values of the SpeculativeBlockUpdate objects that would be
    /// produced by the execution event stream captured in the associated
    /// `event_ring_snapshot`. Specifically, this is a JSON serialized (and
    /// zstd compressed) array of SpeculativeBlockUpdate objects. The data is
    /// produced by the execution daemon, which directly serializes its own
    /// internal data structures in the format used by SpeculativeBlockUpdate
    /// via the serde_json package.
    ///
    /// Note that `struct SpeculativeBlockUpdate` is not part of this library:
    /// it is defined in a downstream package (monad-rpc), which uses this
    /// library as a dependency. We choose to put its test data here because
    /// the two pieces of data are part of the same scenario (these are
    /// precisely the expected updates that correspond to the low-level events
    /// in the event ring snapshot), so they are kept together in a single
    /// object that represents the scenario itself. This field is not used by
    /// any `monad-event-ring` package tests, as it related to downstream
    /// functionality.
    pub expected_block_update_json_zst: &'a [u8],

    /// Sequence number of the last BLOCK_FINALIZE event in the event ring
    /// snapshot
    pub last_block_seqno: u64,
}

// The file "exec-events-emn-30b-15m.zst" contains the execution events
// produced by the execution daemon during a historical replay of the
// Ethereum mainnet (emn) for a run of 30 blocks (30b), starting at block
// 15,000,000 (15m). The file "expected-block-updates-emn-30b-15m.json.zst"
// contains the expected block updates as exported by the execution daemon
// from that same sequence of events.
pub const ETHEREUM_MAINNET_30B_15M: ExecEventTestScenario = ExecEventTestScenario {
    name: "ETHEREUM_MAINNET_30B_15M",
    event_ring_snapshot_zst: include_bytes!("test_data/exec-events-emn-30b-15m.zst"),
    expected_block_update_json_zst: include_bytes!(
        "test_data/expected-block-updates-emn-30b-15m.json.zst"
    ),
    last_block_seqno: 44000,
};

// The file "exec-events-mdn-21b-genesis.zst" contains the execution events
// produced by the execution daemon during a test sequence on the Monad
// devnet (mdn) for a run of 21 blocks (21b), starting from the genesis
// block. This sequence is notable for its presence of duplicate proposals
// and proposals which do not become finalized.
// TODO(ken): we should use reference data that's easy to regenerate; this
//    was hand-crafted for me by Aashish, and is not part of the record of
//    any public blockchain
pub const MONAD_DEVNET_21B_GENESIS: ExecEventTestScenario = ExecEventTestScenario {
    name: "MONAD_DEVNET_21B_GENESIS",
    event_ring_snapshot_zst: include_bytes!("test_data/exec-events-mdn-21b-genesis.zst"),
    expected_block_update_json_zst: include_bytes!(
        "test_data/expected-block-updates-mdn-21b-genesis.json.zst"
    ),
    last_block_seqno: 73350,
};

/// Represents a single event in the cross-validation test. The CVT test
/// is defined in the following way:
///
/// - The execution daemon can export a JSON view of what it thinks a Rust
///   CrossValidationTestUpdate should look like. Because all the major
///   structures in the `exec_event_test_util.rs` module use
///   #[derive(serde::Deserialize)], Rust can easily load the serialized
///   C++ "ground truth."
///
/// - A test starts by loading a persisted snapshot of the event ring file
///   that has been saved to disk, using the event ring's "test utility"
///   module. This snapshot should contain the same low-level event stream for
///   which C++ exported the "ground truth" JSON for CrossValidationTestUpdate
///   objects.
///
/// - The CrossValidationTestStream uses the ExecEventStream, BlockBuilder,
///   and ConsensusStateTracker objects to reassemble the persisted event
///   stream into its own view of the CrossValidationTestUpdate stream.
///
/// - As a final step, we zip the two Vec<CrossValidationTestUpdate> inputs
///   together and check that each pair matches exactly
///
/// The idea behind the cross-validation test is that we're computing the
/// same thing in two very different ways, one of which is very circuitous
/// (execution recorder -> shared memory -> Rust event library -> Rust
/// reassembly library), the other of which is direct and does not use any
/// of the same code.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum CrossValidationTestUpdate {
    Executed(Box<ExecutedBlockInfo>),
    Referendum {
        proposal_meta: ProposalMetadata,
        outcome: ConsensusState,
        superseded_proposals: Vec<ProposalMetadata>,
    },
    UnknownProposal {
        block_number: u64,
        block_id: MonadBlockId,
        consensus_state: ConsensusState,
    },
    UnexpectedEventError(String),
}

pub struct CrossValidationTestStream<'ring> {
    event_stream: ExecEventStream<'ring>,
    block_builder: BlockBuilder,
    consensus_tracker: ConsensusStateTracker<()>,
    unexpected_event_error: Option<String>,
}

impl<'ring> CrossValidationTestStream<'ring> {
    pub fn new(event_ring: &'ring EventRing) -> Result<CrossValidationTestStream<'ring>, String> {
        let mut event_reader = EventReader::new(
            event_ring,
            EventRingType::Exec,
            &EXEC_EVENT_DOMAIN_METADATA.metadata_hash,
        )?;
        event_reader.read_last_seqno = 0;

        let config = ExecEventStreamConfig {
            parse_txn_input: true,
            opt_process_exit_monitor: None,
        };

        Ok(CrossValidationTestStream {
            event_stream: ExecEventStream::new(event_reader, config),
            block_builder: BlockBuilder::new(),
            consensus_tracker: ConsensusStateTracker::new(),
            unexpected_event_error: None,
        })
    }

    pub fn poll(&mut self) -> Option<CrossValidationTestUpdate> {
        if let Some(e) = self.unexpected_event_error.clone() {
            return Some(CrossValidationTestUpdate::UnexpectedEventError(e));
        }

        loop {
            let exec_event = match self.event_stream.poll() {
                PollResult::NotReady => {
                    if self.event_stream.reader().get_available_descriptors() == 0 {
                        return None;
                    }
                    continue;
                }
                r @ PollResult::Disconnected
                | r @ PollResult::Gap { .. }
                | r @ PollResult::PayloadExpired { .. } => {
                    // As soon as we see one of these, we'll fail the cross validation test
                    let e = format!("event stream error: {r:?}");
                    self.unexpected_event_error.replace(e);
                    return Some(CrossValidationTestUpdate::UnexpectedEventError(
                        self.unexpected_event_error.as_ref().unwrap().clone(),
                    ));
                }
                PollResult::Ready { seqno: _, event } => event,
            };
            if let Some(u) = self.try_append(exec_event) {
                return Some(u);
            }
        }
    }

    fn try_append(&mut self, exec_event: ExecEvent) -> Option<CrossValidationTestUpdate> {
        match self.block_builder.try_append(exec_event) {
            Some(BlockUpdate::Executed(exec_info)) => {
                let proposal_meta = exec_info.proposal_meta;
                let consensus_state = exec_info.consensus_state;
                self.consensus_tracker
                    .add_proposal(proposal_meta, consensus_state, ());
                Some(CrossValidationTestUpdate::Executed(exec_info))
            }

            Some(BlockUpdate::NonBlockEvent(ExecEvent::Referendum {
                proposal_meta,
                outcome,
            })) => {
                // A consensus referendum on a proposal has concluded; look up the associated
                // proposal
                let block_number = proposal_meta.block_number;
                let block_id = proposal_meta.id;
                match self
                    .consensus_tracker
                    .update_proposal(block_number, &block_id, outcome)
                {
                    ConsensusStateResult::Outstanding { .. } => {
                        Some(CrossValidationTestUpdate::Referendum {
                            proposal_meta,
                            outcome,
                            superseded_proposals: Vec::new(),
                        })
                    }

                    ConsensusStateResult::Finalization {
                        finalized_proposal: _,
                        abandoned_proposals,
                    } => {
                        let mut superseded_proposals: Vec<ProposalMetadata> = abandoned_proposals
                            .into_iter()
                            .map(|ps| ps.proposal_meta)
                            .collect();
                        superseded_proposals.sort_by(|lhs, rhs| lhs.id.0.cmp(&rhs.id.0));
                        Some(CrossValidationTestUpdate::Referendum {
                            proposal_meta,
                            outcome,
                            superseded_proposals,
                        })
                    }

                    ConsensusStateResult::Verification { .. } => {
                        Some(CrossValidationTestUpdate::Referendum {
                            proposal_meta,
                            outcome,
                            superseded_proposals: Vec::new(),
                        })
                    }

                    ConsensusStateResult::UnknownProposal { .. } => {
                        Some(CrossValidationTestUpdate::UnknownProposal {
                            block_number: proposal_meta.block_number,
                            block_id: proposal_meta.id,
                            consensus_state: outcome,
                        })
                    }
                }
            }

            Some(bu @ BlockUpdate::Failed(_))
            | Some(bu @ BlockUpdate::OrphanedEvent(_))
            | Some(bu @ BlockUpdate::ImplicitDrop { .. }) => {
                let e = format!("BlockUpdate we don't expect execution CVT to produce: {bu:?}");
                self.unexpected_event_error.replace(e);
                Some(CrossValidationTestUpdate::UnexpectedEventError(
                    self.unexpected_event_error.as_ref().unwrap().clone(),
                ))
            }

            Some(BlockUpdate::NonBlockEvent(_)) | None => None,
        }
    }
}

#[cfg(test)]
mod test {
    use monad_event_ring::{event_ring::EventRing, event_ring_util::EventRingSnapshot};

    use crate::{
        exec_event_test_util::{
            CrossValidationTestStream, CrossValidationTestUpdate, ETHEREUM_MAINNET_30B_15M,
        },
        exec_events::ConsensusState,
    };

    #[test]
    fn cvt_emn_30b_15m() {
        let expected_file_bits = ETHEREUM_MAINNET_30B_15M.expected_block_update_json_zst;

        const MAX_FILE_SIZE: usize = 1 << 30;
        let zstd_decomp_result = zstd::bulk::decompress(expected_file_bits, MAX_FILE_SIZE);

        // Deserialize the expected updates from the input file
        let mut expected_block_updates: Vec<CrossValidationTestUpdate> =
            serde_json::from_slice(zstd_decomp_result.unwrap().as_slice()).unwrap();

        // For the actual updates, we'll open the compressed snapshot of the
        // underlying events
        let actual_snap = EventRingSnapshot::load_from_bytes(
            ETHEREUM_MAINNET_30B_15M.event_ring_snapshot_zst,
            ETHEREUM_MAINNET_30B_15M.name,
        );
        let mmap_prot = libc::PROT_READ;
        let mmap_extra_flags = 0;
        let event_ring = match EventRing::mmap_from_fd(
            mmap_prot,
            mmap_extra_flags,
            actual_snap.snapshot_fd,
            actual_snap.snapshot_off,
            ETHEREUM_MAINNET_30B_15M.name,
        ) {
            Err(e) => {
                panic!("event library error -- {e}");
            }
            Ok(r) => r,
        };

        let mut cvt_stream = match CrossValidationTestStream::new(&event_ring) {
            Err(e) => {
                panic!("event library error -- {e}");
            }
            Ok(s) => s,
        };

        let mut actual_block_updates: Vec<CrossValidationTestUpdate> = Vec::new();
        while let Some(u) = cvt_stream.poll() {
            if let CrossValidationTestUpdate::UnexpectedEventError(e) = u {
                panic!("unexpected event error occurred, CVT test failed: {e}");
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
            assert_eq!(*expected_update, *actual_update);
            matched_update_count += 1;
        }

        println!("matched all {matched_update_count} block updates");
    }
}
