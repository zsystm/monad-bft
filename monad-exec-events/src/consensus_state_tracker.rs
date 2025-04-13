//! This module provides a utility (ConsensusStateTracker) that tracks the
//! consensus state of a proposal as it changes

use std::{
    collections::{hash_map, HashMap},
    sync::{Arc, RwLock},
};

use crate::exec_events::{ConsensusState, MonadBlockId, ProposalMetadata};

/// Stores the current consensus state of an outstanding block proposal, and
/// the user-provided data that is associated with it. Once the block is either
/// verified or abandoned, `current_state` will no longer be modified.
///
/// ProposalState lifetime is managed using Arc; while a proposal is still
/// outstanding, its proposal state is jointly owned by the
/// ConsensusStateTracker and any additional reference counts the user takes
/// on the ProposalState object. Once the proposal is verified or abandoned,
/// the ConsensusStateTracker releases its reference count on the state,
/// leaving it to be solely owned by the user.
#[derive(Debug)]
pub struct ProposalState<T> {
    pub proposal_meta: ProposalMetadata,
    pub current_state: RwLock<ConsensusState>,
    pub user_data: T,
}

/// When tracking a new proposal or updating a proposal with the outcome
/// of a referendum, this result is returned to indicate what happened
/// both to the proposal itself, and to any other proposals sharing the
/// same block number.
pub enum ConsensusStateResult<T> {
    /// A proposal was added (in either the Proposed or QC initial state)
    /// or was transitioned to the QC state by a referendum; if there
    /// are any competing proposals to become the same block number, they
    /// are also returned
    Outstanding {
        updated_proposal: Arc<ProposalState<T>>,
        other_proposals: Vec<Arc<ProposalState<T>>>,
    },

    /// A proposal has become the canonical block for a given block number;
    /// it remains in force because it must still be verified. This action
    /// also implicitly abandons all other proposals competing for
    /// commitment with the same block number. Their reference counts are
    /// removed internally, leaving the caller as the sole owner(s).
    Finalization {
        finalized_proposal: Arc<ProposalState<T>>,
        abandoned_proposals: Vec<Arc<ProposalState<T>>>,
    },

    /// A finalized proposal has been verified
    Verification(Arc<ProposalState<T>>),

    /// Returned when the user attempts to update the consensus state of
    /// a proposal that was never added
    UnknownProposal {
        block_number: u64,
        block_id: MonadBlockId,
        consensus_state: ConsensusState,
    },
}

/// Tracks executed block proposals; when the Monad consensus network passes
/// a referendum about a block proposal, this utility is informed and all
/// changes to the associated block height are returned. This is primarily
/// needed to simplify finalization: when a proposal of block number `h` is
/// finalized, all competing proposals with that same height are implicitly
/// abandoned. This utility makes that abandonment explicit (all abandoned
/// proposal state objects are returned to the caller), so the user can more
/// easily determine when to destroy any extra state they hold for the
/// proposal if it does not finalize.
#[derive(Clone, Debug)]
pub struct ConsensusStateTracker<T> {
    proposal_state_map: HashMap<u64, Vec<Arc<ProposalState<T>>>>,
}

impl<T> ConsensusStateTracker<T> {
    pub fn new() -> Self {
        ConsensusStateTracker {
            proposal_state_map: HashMap::new(),
        }
    }

    /// Begin tracking a new proposal
    pub fn add_proposal(
        &mut self,
        proposal_meta: ProposalMetadata,
        initial_state: ConsensusState,
        t: T,
    ) -> ConsensusStateResult<T> {
        match self.proposal_state_map.entry(proposal_meta.block_number) {
            hash_map::Entry::Occupied(mut proposal_tokens) => {
                // If the data structure is maintained correctly, occupied
                // implies the vector must have at least one element
                assert!(!proposal_tokens.get().is_empty());

                if let Some(dup) = proposal_tokens
                    .get()
                    .iter()
                    .find(|ps| ps.proposal_meta.id == proposal_meta.id)
                {
                    // In the current system, duplicate proposals can be
                    // executed. In a future version of the execution
                    // daemon, this may be fixed.
                    return ConsensusStateResult::Outstanding {
                        updated_proposal: dup.clone(),
                        other_proposals: proposal_tokens
                            .get()
                            .iter()
                            .filter(|ps| ps.proposal_meta.id != proposal_meta.id)
                            .cloned()
                            .collect(),
                    };
                }

                match initial_state {
                    ConsensusState::Proposed | ConsensusState::QC => {
                        proposal_tokens.get_mut().push(Arc::new(ProposalState {
                            proposal_meta,
                            current_state: RwLock::new(initial_state),
                            user_data: t,
                        }));
                        let proposal_tokens = proposal_tokens.get();
                        ConsensusStateResult::Outstanding {
                            updated_proposal: proposal_tokens[proposal_tokens.len() - 1].clone(),
                            other_proposals: proposal_tokens[..proposal_tokens.len() - 1].to_vec(),
                        }
                    }
                    ConsensusState::Finalized => {
                        let proposals = proposal_tokens.remove_entry();
                        self.handle_finalized(&proposal_meta.id, proposals)
                    }
                    ConsensusState::Verified => {
                        let proposals = proposal_tokens.remove_entry();
                        self.handle_verified(proposals)
                    }
                }
            }
            hash_map::Entry::Vacant(v) => {
                let inserted: &Vec<Arc<ProposalState<T>>> =
                    v.insert(vec![Arc::new(ProposalState {
                        proposal_meta,
                        current_state: RwLock::new(initial_state),
                        user_data: t,
                    })]);
                ConsensusStateResult::Outstanding {
                    updated_proposal: inserted[0].clone(),
                    other_proposals: Vec::new(),
                }
            }
        }
    }

    /// Update the consensus state of an existing proposal
    pub fn update_proposal(
        &mut self,
        block_number: u64,
        block_id: &MonadBlockId,
        new_state: ConsensusState,
    ) -> ConsensusStateResult<T> {
        match self.proposal_state_map.entry(block_number) {
            hash_map::Entry::Occupied(proposal_tokens) => {
                match new_state {
                    ConsensusState::Proposed => {
                        panic!("new_state == Proposed must call add_proposal instead")
                    }
                    ConsensusState::QC => {
                        let proposal_tokens = proposal_tokens.get();
                        let (updated, remaining): (Vec<_>, Vec<_>) = proposal_tokens
                            .iter()
                            .partition(|pt| pt.proposal_meta.id == *block_id);

                        match updated.len() {
                            0 => ConsensusStateResult::UnknownProposal {
                                block_number,
                                block_id: *block_id,
                                consensus_state: new_state,
                            },
                            1 => {
                                *updated[0].current_state.write().unwrap() = ConsensusState::QC;
                                ConsensusStateResult::Outstanding {
                                    updated_proposal: updated[0].clone(),
                                    other_proposals: remaining.into_iter().cloned().collect(),
                                }
                            }
                            _ => {
                                let mut dump = String::new();
                                for (block, proposals) in self.proposal_state_map.iter() {
                                    for ps in proposals {
                                        dump.push_str(&format!(
                                            "\n{} => M:{:#?} S:{:#?}",
                                            block,
                                            ps.proposal_meta,
                                            ps.current_state.read().unwrap()
                                        ));
                                    }
                                }
                                panic!("duplicate block id {block_id:?}, dump of full state map:{dump}");
                            }
                        }
                    }
                    ConsensusState::Finalized => {
                        let proposals = proposal_tokens.remove_entry();
                        self.handle_finalized(block_id, proposals)
                    }
                    ConsensusState::Verified => {
                        let proposals = proposal_tokens.remove_entry();
                        self.handle_verified(proposals)
                    }
                }
            }
            hash_map::Entry::Vacant(_) => ConsensusStateResult::UnknownProposal {
                block_number,
                block_id: *block_id,
                consensus_state: new_state,
            },
        }
    }

    /// Get the proposal state for a given block number and ID, if one is
    /// available
    pub fn get(&self, block_number: u64, block_id: &MonadBlockId) -> Option<Arc<ProposalState<T>>> {
        if let Some(pending_block_tokens) = self.proposal_state_map.get(&block_number) {
            pending_block_tokens
                .iter()
                .find(|pbt| pbt.proposal_meta.id == *block_id)
                .cloned()
        } else {
            None
        }
    }

    pub fn reset_all(&mut self) {
        self.proposal_state_map.clear();
    }

    fn handle_finalized(
        &mut self,
        finalized_block_id: &MonadBlockId,
        entry: (u64, Vec<Arc<ProposalState<T>>>),
    ) -> ConsensusStateResult<T> {
        let (block_number, proposal_tokens) = entry;
        let (finalized_proposals, abandoned_proposals): (Vec<_>, Vec<_>) = proposal_tokens
            .into_iter()
            .partition(|pt| pt.proposal_meta.id == *finalized_block_id);

        match finalized_proposals.len() {
            0 => ConsensusStateResult::UnknownProposal {
                block_number,
                block_id: *finalized_block_id,
                consensus_state: ConsensusState::Finalized,
            },
            1 => {
                let finalized_proposal = finalized_proposals[0].clone();
                self.proposal_state_map
                    .insert(block_number, finalized_proposals); // Wait to be verified
                *finalized_proposal.current_state.write().unwrap() = ConsensusState::Finalized;
                ConsensusStateResult::Finalization {
                    finalized_proposal,
                    abandoned_proposals,
                }
            }
            _ => panic!("duplicate block id {finalized_block_id:?}"),
        }
    }

    fn handle_verified(
        &mut self,
        entry: (u64, Vec<Arc<ProposalState<T>>>),
    ) -> ConsensusStateResult<T> {
        let (_, mut proposal_tokens) = entry;
        assert_eq!(proposal_tokens.len(), 1usize);
        let verified = proposal_tokens.pop().unwrap();
        *verified.current_state.write().unwrap() = ConsensusState::Verified;
        ConsensusStateResult::Verification(verified)
    }
}

impl<T> Default for ConsensusStateTracker<T> {
    fn default() -> Self {
        Self::new()
    }
}
