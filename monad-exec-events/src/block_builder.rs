//! This module provides a utility to reassemble fine-grained real time events
//! (from the `exec_events.rs` module) into "block-oriented" updates. That is,
//! all execution events pertaining to a particular block (e.g., all emitted
//! logs, all receipts, etc.) are aggregated and returned as a single object,
//! for users that prefer a block-at-a-time API.

use alloy_primitives::{B256, B64, U256};

use crate::exec_events::{
    CallFrame, ConsensusState, EthBlockExecInput, ExecEvent, ProposalMetadata,
};

/// Fine-grained events from the execution daemon are aggregated into
/// block-at-a-time updates. When all events pertaining to a block's
/// execution have been encountered (or if an error occurs before that
/// happens), one of these "block update" objects is produced.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum BlockUpdate {
    /// Execution information for a block which failed to execute
    Failed(Box<FailedBlockInfo>),

    /// Execution information for a block whose execution succeeded; the block
    /// has reached the specified consensus state. If the consensus state is
    /// not Verified, the caller may wish to pay attention to future
    /// ExecUpdate::Referendum events pertaining to the same proposal. Such
    /// events are not scoped to any block; if passed to the BlockBuilder,
    /// they will be immediately returned using the NonBlockEvent variant
    Executed(Box<ExecutedBlockInfo>),

    /// An event occurred outside the scope of a block, or is otherwise not
    /// related to the current block; ownership of the event is returned to
    /// the caller without it being added to the current block
    NonBlockEvent(ExecEvent),

    /// An event which is normally scoped to the current block is returned to
    /// the caller using this variant, if there is no active block being
    /// reassembled. This is the common case during program startup: if the
    /// execution daemon is already running when we start listening to events,
    /// the first events we see are likely to occur in the middle of some
    /// block's execution; we will not have seen that block's BlockStart event.
    /// The typical course of action is to ignore these, but they're explicitly
    /// returned to the caller for the sake of completeness
    OrphanedEvent(ExecEvent),

    /// Variant used when a block must be dropped because it can be reassembled
    /// correctly; the user can prevent these from being created by detecting
    /// recoverable event stream errors themselves (gaps, etc.) and calling
    /// "drop_block" to discard the current in-process block; these will never
    /// occur unless events are missing
    ImplicitDrop {
        dropped_block: Box<ExecutedBlockInfo>,
        reassembly_error: ReassemblyError,
    },
}

/// Collection of all event information aggregated during the execution
/// of a single block
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExecutedBlockInfo {
    pub consensus_state: ConsensusState,
    pub proposal_meta: ProposalMetadata,
    pub chain_id: alloy_primitives::ChainId,
    pub eth_header: alloy_consensus::Header,
    pub eth_block_hash: B256,
    pub transactions: Vec<Option<TransactionInfo>>,
}

/// Collection of summary information that is available about a failed
/// block execution
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FailedBlockInfo {
    pub consensus_state: ConsensusState,
    pub proposal_meta: ProposalMetadata,
    pub chain_id: alloy_primitives::ChainId,
    pub eth_header: alloy_consensus::Header,
    pub failure_reason: BlockExecutionFailure,
}

/// Encodes the reason that a block failed to fully execute
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub enum BlockExecutionFailure {
    /// Attempted to execute a block that failed validation; the numerical
    /// code corresponds to a `BlockError` enumeration constant in
    /// `validate_block.hpp` in the execution daemon source code
    RejectedBlock { reject_code: u32 },

    /// Attempted to execute a transaction that failed validation; the
    /// numerical code corresponds to a `TransactionError` enumeration
    /// constant in `validate_transaction.hpp` in the execution daemon
    /// source code
    RejectedTransaction { index: u64, reject_code: u32 },

    /// Domain ID and error code from the execution daemon's error
    /// reporting system
    ExecutionDaemonInternal {
        tx_id: u64,
        domain_id: u64,
        status_code: i64,
    },
}

/// All info about each transaction that occurs in a block
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TransactionInfo {
    pub index: alloy_primitives::TxIndex,
    pub tx_envelope: alloy_consensus::TxEnvelope,
    pub sender: alloy_primitives::Address,
    pub receipt: alloy_consensus::Receipt,
    pub tx_gas_used: u128,
    pub call_frames: Vec<CallFrame>,
}

/// Error that occurs when we can detect that an event is missing, e.g.,
/// at the end of the block we didn't see all the transactions
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ReassemblyError {
    /// We encountered a new BlockStart before the current block completed;
    /// that new BlockStart is returned, so that it can be retried
    UnterminatedBlock { block_start: ExecEvent },

    /// Transaction with the given index is missing
    MissingTransaction { index: u64 },

    /// Transaction with the given index does not have the expected number
    /// of logs
    MissingTransactionLog {
        index: u64,
        expected_log_count: usize,
        actual_log_count: usize,
    },
}

#[derive(Debug)]
struct ReassemblyState {
    log_count: Vec<usize>,
}

/// Object which captures low-level execution events and builds complete
/// BlockUpdate objects out of them
pub struct BlockBuilder {
    current_block: Option<Box<ExecutedBlockInfo>>,
    current_reassembly_state: Option<ReassemblyState>,
}

impl Default for BlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockBuilder {
    pub fn new() -> BlockBuilder {
        BlockBuilder {
            current_block: None,
            current_reassembly_state: None,
        }
    }

    /// Try to append an execution event to the current block. In most cases
    /// this performs the append and returns None, indicating no full block
    /// update is ready yet. If the append completes the current block (i.e.,
    /// if the event represents the last event in a block evaluation), a
    /// BlockUpdate will be produced. A BlockUpdate will also be produced
    /// in the case of sequencing errors (ImplicitDrop and OrphanedEvent
    /// variants) or if the event is not scoped to any particular block
    /// (NonBlockEvent variant).
    pub fn try_append(&mut self, event: ExecEvent) -> Option<BlockUpdate> {
        match event {
            ExecEvent::BlockStart {
                consensus_state,
                proposal_meta,
                chain_id,
                exec_input,
            } => {
                if let Some(dropped_block) = self.current_block.take() {
                    // BlockStart, but there's still a block actively being
                    // reassembled; this causes the active block to be dropped,
                    // with its currently-reassembled (but incomplete) state
                    // returned to the caller. Some fields will be incomplete,
                    // e.g., the state_root in the Ethereum header will still
                    // be B256::ZERO, since we didn't see the block finish.
                    //
                    // This also does _not_ process the new BlockStart, i.e.,
                    // it does not start a new block. Instead, it returns the
                    // BlockStart event back to the caller, so they have option
                    // to react to this situation and try again. Trying again
                    // will succeed, since the old block is now gone.
                    return Some(BlockUpdate::ImplicitDrop {
                        dropped_block,
                        reassembly_error: ReassemblyError::UnterminatedBlock {
                            block_start: ExecEvent::BlockStart {
                                consensus_state,
                                proposal_meta,
                                chain_id,
                                exec_input,
                            },
                        },
                    });
                }

                let txn_count = exec_input.transaction_count as usize;
                let mut reassembly_state = ReassemblyState {
                    log_count: Vec::with_capacity(txn_count),
                };
                reassembly_state.log_count.resize(txn_count, 0);
                self.current_reassembly_state.replace(reassembly_state);

                self.current_block.replace(Box::new(ExecutedBlockInfo {
                    consensus_state,
                    proposal_meta,
                    chain_id,
                    eth_header: create_alloy_block_header(exec_input),
                    eth_block_hash: B256::default(),
                    transactions: vec![None; txn_count],
                }));
                None
            }

            ExecEvent::BlockEnd {
                eth_block_hash,
                state_root,
                receipts_root,
                logs_bloom,
                gas_used,
            } => {
                if let Some(mut cur_block) = self.current_block.take() {
                    cur_block.eth_block_hash = eth_block_hash;
                    let eth_header = &mut cur_block.eth_header;
                    eth_header.state_root = state_root;
                    eth_header.receipts_root = receipts_root;
                    eth_header.logs_bloom = *logs_bloom;
                    eth_header.gas_used = gas_used;
                    let mut cumulative_gas_used = 0;
                    for (index, opt_txn_info) in &mut cur_block.transactions.iter_mut().enumerate()
                    {
                        let txn_reassembly_state = self.current_reassembly_state.as_ref().unwrap();

                        if let Some(txn_info) = opt_txn_info {
                            // An unknown log is missing
                            let expected_log_count = txn_reassembly_state.log_count[index];
                            let actual_log_count = txn_info.receipt.logs.len();

                            if actual_log_count != expected_log_count {
                                return Some(BlockUpdate::ImplicitDrop {
                                    dropped_block: cur_block,
                                    reassembly_error: ReassemblyError::MissingTransactionLog {
                                        index: index as u64,
                                        expected_log_count,
                                        actual_log_count,
                                    },
                                });
                            }
                            cumulative_gas_used += txn_info.tx_gas_used;
                            txn_info.receipt.cumulative_gas_used = cumulative_gas_used;
                        } else {
                            // This transaction is missing
                            return Some(BlockUpdate::ImplicitDrop {
                                dropped_block: cur_block,
                                reassembly_error: ReassemblyError::MissingTransaction {
                                    index: index as u64,
                                },
                            });
                        }
                    }
                    Some(BlockUpdate::Executed(cur_block))
                } else {
                    None
                }
            }

            ExecEvent::BlockReject { reject_code } => {
                self.abort_block(BlockExecutionFailure::RejectedBlock { reject_code })
            }

            ExecEvent::TransactionStart {
                index,
                sender,
                tx_envelope,
            } => {
                if let Some(block_info) = self.current_block.as_mut() {
                    block_info.transactions[index as usize].replace(TransactionInfo {
                        index,
                        tx_envelope,
                        sender,
                        receipt: alloy_consensus::Receipt::default(),
                        tx_gas_used: 0,
                        call_frames: Vec::new(),
                    });
                    None
                } else {
                    Some(BlockUpdate::OrphanedEvent(ExecEvent::TransactionStart {
                        index,
                        sender,
                        tx_envelope,
                    }))
                }
            }

            ExecEvent::TransactionReceipt {
                index,
                status,
                log_count,
                call_frame_count,
                tx_gas_used,
            } => {
                if let Some(block_info) = self.current_block.as_mut() {
                    let txn = block_info.transactions[index as usize].as_mut().unwrap();
                    txn.receipt.status = status;
                    txn.receipt.logs.reserve(log_count);
                    txn.call_frames.reserve(call_frame_count);
                    // We can't compute cumulative gas used until the block
                    // completes, since transactions may be recorded
                    // out-of-order, even though they're committed in order
                    txn.tx_gas_used = tx_gas_used;
                    self.current_reassembly_state.as_mut().unwrap().log_count[index as usize] =
                        log_count;
                    None
                } else {
                    Some(BlockUpdate::OrphanedEvent(ExecEvent::TransactionReceipt {
                        index,
                        status,
                        log_count,
                        call_frame_count,
                        tx_gas_used,
                    }))
                }
            }

            ExecEvent::TransactionLog { index, log } => {
                if let Some(block_info) = self.current_block.as_mut() {
                    let txn = block_info.transactions[index as usize].as_mut().unwrap();
                    txn.receipt.logs.push(log);
                    None
                } else {
                    Some(BlockUpdate::OrphanedEvent(ExecEvent::TransactionLog {
                        index,
                        log,
                    }))
                }
            }

            ExecEvent::TransactionCallFrame { index, call_frame } => {
                if let Some(block_info) = self.current_block.as_mut() {
                    let txn = block_info.transactions[index as usize].as_mut().unwrap();
                    txn.call_frames.push(call_frame);
                    None
                } else {
                    Some(BlockUpdate::OrphanedEvent(
                        ExecEvent::TransactionCallFrame { index, call_frame },
                    ))
                }
            }

            ExecEvent::TransactionReject { index, reject_code } => {
                self.abort_block(BlockExecutionFailure::RejectedTransaction { index, reject_code })
            }

            ExecEvent::EvmError {
                domain_id,
                status_code,
                tx_id,
            } => self.abort_block(BlockExecutionFailure::ExecutionDaemonInternal {
                tx_id,
                domain_id,
                status_code,
            }),

            other => Some(BlockUpdate::NonBlockEvent(other)),
        }
    }

    /// Discards the current block that is being reassembled; this should be
    /// called when the underlying event stream is interrupted. If it is
    /// not called, try_append may return the BlockUpdate::ImplicitDrop variant
    pub fn drop_block(&mut self) -> Option<Box<ExecutedBlockInfo>> {
        self.take_current_block()
    }

    fn take_current_block(&mut self) -> Option<Box<ExecutedBlockInfo>> {
        // Helper function to make sure we also discard the reassembly state
        // whenever we take the current block
        self.current_reassembly_state.take();
        self.current_block.take()
    }

    fn abort_block(&mut self, failure_reason: BlockExecutionFailure) -> Option<BlockUpdate> {
        self.take_current_block().map(|cur_block| {
            BlockUpdate::Failed(Box::new(FailedBlockInfo {
                consensus_state: cur_block.consensus_state,
                proposal_meta: cur_block.proposal_meta,
                chain_id: cur_block.chain_id,
                eth_header: cur_block.eth_header,
                failure_reason,
            }))
        })
    }
}

fn create_alloy_block_header(exec_input: EthBlockExecInput) -> alloy_consensus::Header {
    alloy_consensus::Header {
        parent_hash: exec_input.parent_hash,
        ommers_hash: exec_input.ommers_hash,
        beneficiary: exec_input.beneficiary,
        state_root: B256::ZERO,
        transactions_root: exec_input.transactions_root,
        receipts_root: B256::ZERO,
        logs_bloom: alloy_primitives::Bloom::ZERO,
        difficulty: U256::from(exec_input.difficulty),
        number: exec_input.number,
        gas_limit: exec_input.gas_limit,
        gas_used: 0,
        timestamp: exec_input.timestamp,
        extra_data: exec_input.extra_data,
        mix_hash: exec_input.prev_randao,
        nonce: B64::from(exec_input.nonce),
        base_fee_per_gas: exec_input.base_fee_per_gas.map(|f| f.to::<u64>()),
        withdrawals_root: exec_input.withdrawals_root,
        blob_gas_used: None,
        excess_blob_gas: None,
        parent_beacon_block_root: None,
        requests_hash: None,
        target_blobs_per_block: None,
    }
}
