//! This module provides a utility to reassemble fine-grained real time events
//! (from the `exec_events.rs` module) into "block-oriented" updates. That is,
//! all execution events pertaining to a particular block (e.g., all emitted
//! logs, all call frames, etc.) are aggregated and returned as a single
//! "block update", for users that prefer a block-at-a-time API.

use std::collections::{hash_map, HashMap};

use alloy_primitives::{Address, B256, B64, U256};

use crate::exec_events::{
    AccountAccess, AccountAccessContext, CallFrame, ConsensusState, EthBlockExecInput, ExecEvent,
    ProposalMetadata, StorageAccess,
};

/// Fine-grained events from the execution daemon are aggregated into
/// block-at-a-time updates. When all events pertaining to a block's
/// execution have been encountered (or if an error occurs before that
/// happens), one of these "block update" objects is produced.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum BlockUpdate {
    /// Execution information for a block whose execution succeeded; the block
    /// has reached the specified consensus state. If the consensus state is
    /// not Verified, the caller may wish to pay attention to future
    /// ExecUpdate::Referendum events pertaining to the same proposal. Such
    /// events are not scoped to any block; if passed to the BlockBuilder,
    /// they will be immediately returned using the NonBlockEvent variant
    Executed(Box<ExecutedBlockInfo>),

    /// Execution information for a block which failed to execute
    Failed(Box<FailedBlockInfo>),

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

    /// Variant used when a block must be dropped because it cannot be
    /// reassembled correctly; the user can prevent these from being created by
    /// detecting recoverable event stream errors themselves (gaps, etc.) and
    /// calling the "drop_block" function to discard the current in-process
    /// block; this variant will never be created unless events are missing
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
    pub prologue_account_accesses: HashMap<Address, AccountAccessInfo>,
    pub epilogue_account_accesses: HashMap<Address, AccountAccessInfo>,
    pub all_account_accesses: HashMap<Address, AccountAccessInfo>,
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

    /// A transaction failed validation before attempting to execute it; the
    /// numerical code corresponds to a `TransactionError` enumeration
    /// constant in `validate_transaction.hpp` in the execution daemon
    /// source code
    RejectedTransaction { txn_index: u64, reject_code: u32 },

    /// Domain ID and error code from the execution daemon's error
    /// reporting system
    ExecutionDaemonInternal {
        txn_id: u64,
        domain_id: u64,
        status_code: i64,
    },
}

/// All info about each transaction that occurs in a block
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TransactionInfo {
    pub txn_index: alloy_primitives::TxIndex,
    pub txn_envelope: alloy_consensus::TxEnvelope,
    pub sender: Address,
    pub receipt: alloy_consensus::Receipt,
    pub txn_gas_used: u128,
    pub call_frames: Vec<CallFrame>,
    pub account_accesses: HashMap<Address, AccountAccessInfo>,
}

/// All info about accesses/changes to an account occurring in some scope,
/// including all the storage and transient storage accesses
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AccountAccessInfo {
    pub original_nonce: u64,
    pub modified_nonce: Option<u64>,
    pub original_balance: U256,
    pub modified_balance: Option<U256>,
    pub code_hash: B256,
    pub storage_accesses: HashMap<B256, StorageSlot>,
    pub transient_accesses: HashMap<B256, StorageSlot>,
}

/// Representation of the state of a storage slot that was read or modified;
/// if the storage for this key was not modified, the optional value will be
/// [`None`]; if it was explicitly deleted, it will be [`B256::ZERO`]
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StorageSlot {
    pub original_value: B256,
    pub modified_value: Option<B256>,
}

/// Error that occurs when we can detect that an event is missing, e.g.,
/// at the end of the block we didn't see all the transactions
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ReassemblyError {
    /// We encountered a new BlockStart before the current block completed;
    /// that new BlockStart is returned, so that it can be retried
    UnterminatedBlock { block_start: ExecEvent },

    /// Transaction with the given index is missing
    MissingTransaction { txn_index: u64 },

    /// Array with an element that is missing; this does not provide the
    /// missing element index; the caller should make sure to call
    /// `drop_block` when necessary, to avoid creating these
    MissingArrayElement {
        location: ArrayElementLocation,
        expected_element_count: usize,
        actual_element_count: usize,
    },
}

#[derive(Clone, Debug, Default)]
struct AccountReassemblyState {
    address: Address,
    storage_keys_count: u32,
    transient_keys_count: u32,
}

#[derive(Clone, Debug, Default)]
struct TransactionReassemblyState {
    expected_log_count: usize,
    expected_call_frame_count: usize,
    account_reassembly_state: Vec<AccountReassemblyState>,
}

#[derive(Debug)]
struct ReassemblyState {
    transactions: Vec<TransactionReassemblyState>,
    prologue_accounts: Vec<AccountReassemblyState>,
    epilogue_accounts: Vec<AccountReassemblyState>,
}

// TODO(ken): this is poorly done
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ArrayElementLocation {
    Log {
        txn_index: u64,
    },
    CallFrame {
        txn_index: u64,
    },
    Account {
        access_context: AccountAccessContext,
    },
    Storage {
        access_context: AccountAccessContext,
        address: Address,
    },
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
    /// update is ready yet. If appending completes the current block (i.e.,
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
                self.current_block.replace(Box::new(ExecutedBlockInfo {
                    consensus_state,
                    proposal_meta,
                    chain_id,
                    eth_header: create_alloy_block_header(exec_input),
                    eth_block_hash: B256::default(),
                    transactions: vec![None; txn_count],
                    prologue_account_accesses: HashMap::new(),
                    epilogue_account_accesses: HashMap::new(),
                    all_account_accesses: HashMap::new(),
                }));
                self.current_reassembly_state.replace(ReassemblyState {
                    transactions: vec![TransactionReassemblyState::default(); txn_count],
                    prologue_accounts: Vec::new(),
                    epilogue_accounts: Vec::new(),
                });
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
                    Some(try_create_block_update(
                        cur_block,
                        self.current_reassembly_state.take().unwrap(),
                    ))
                } else {
                    None
                }
            }

            ExecEvent::BlockReject { reject_code } => {
                self.abort_block(BlockExecutionFailure::RejectedBlock { reject_code })
            }

            txn_event @ ExecEvent::TransactionReceipt { .. }
            | txn_event @ ExecEvent::TransactionLog { .. }
            | txn_event @ ExecEvent::TransactionCallFrame { .. } => {
                self.append_txn_event(txn_event)
            }

            ExecEvent::TransactionStart {
                txn_index,
                sender,
                txn_envelope,
            } => {
                if let Some(block_info) = self.current_block.as_mut() {
                    block_info.transactions[txn_index as usize].replace(TransactionInfo {
                        txn_index,
                        txn_envelope,
                        sender,
                        receipt: alloy_consensus::Receipt::default(),
                        txn_gas_used: 0,
                        call_frames: Vec::new(),
                        account_accesses: HashMap::new(),
                    });
                    None
                } else {
                    Some(BlockUpdate::OrphanedEvent(ExecEvent::TransactionStart {
                        txn_index,
                        sender,
                        txn_envelope,
                    }))
                }
            }

            ExecEvent::TransactionReject {
                txn_index,
                reject_code,
            } => self.abort_block(BlockExecutionFailure::RejectedTransaction {
                txn_index,
                reject_code,
            }),

            ExecEvent::AccountAccessListHeader {
                access_context,
                entry_count,
            } => {
                if let AccountAccessContext::Transaction(_) = access_context {
                    self.append_txn_event(ExecEvent::AccountAccessListHeader {
                        access_context,
                        entry_count,
                    })
                } else if let Some(block_info) = self.current_block.as_mut() {
                    let reassembly_state = self.current_reassembly_state.as_mut().unwrap();
                    let (system_account_access_map, account_reassembly_vec) = match access_context {
                        AccountAccessContext::BlockPrologue => (
                            &mut block_info.prologue_account_accesses,
                            &mut reassembly_state.prologue_accounts,
                        ),
                        AccountAccessContext::BlockEpilogue => (
                            &mut block_info.epilogue_account_accesses,
                            &mut reassembly_state.epilogue_accounts,
                        ),
                        _ => panic!("unknown account access context {access_context:?}"),
                    };
                    std::mem::swap(
                        account_reassembly_vec,
                        &mut vec![AccountReassemblyState::default(); entry_count as usize],
                    );
                    system_account_access_map.reserve(entry_count as usize);
                    None
                } else {
                    Some(BlockUpdate::OrphanedEvent(
                        ExecEvent::AccountAccessListHeader {
                            access_context,
                            entry_count,
                        },
                    ))
                }
            }

            ExecEvent::AccountAccess {
                account_index,
                access_context,
                access_info,
            } => {
                if let AccountAccessContext::Transaction(_) = access_context {
                    self.append_txn_event(ExecEvent::AccountAccess {
                        account_index,
                        access_context,
                        access_info,
                    })
                } else if let Some(block_info) = self.current_block.as_mut() {
                    let reassembly_state = self.current_reassembly_state.as_mut().unwrap();
                    let (system_account_access_map, account_reassembly_vec) = match access_context {
                        AccountAccessContext::BlockPrologue => (
                            &mut block_info.prologue_account_accesses,
                            &mut reassembly_state.prologue_accounts,
                        ),
                        AccountAccessContext::BlockEpilogue => (
                            &mut block_info.epilogue_account_accesses,
                            &mut reassembly_state.epilogue_accounts,
                        ),
                        _ => panic!("unexpected account access context {access_context:?}"),
                    };
                    let account_reassembly = &mut account_reassembly_vec[account_index as usize];
                    account_reassembly.address = access_info.address;
                    account_reassembly.storage_keys_count = access_info.storage_key_count;
                    account_reassembly.transient_keys_count = access_info.transient_key_count;
                    system_account_access_map
                        .insert(access_info.address, make_account_access_info(access_info));
                    None
                } else {
                    Some(BlockUpdate::OrphanedEvent(ExecEvent::AccountAccess {
                        account_index,
                        access_context,
                        access_info,
                    }))
                }
            }

            ExecEvent::StorageAccess {
                access_context,
                account_index,
                storage_index,
                access_info,
            } => {
                if let AccountAccessContext::Transaction(_) = access_context {
                    self.append_txn_event(ExecEvent::StorageAccess {
                        access_context,
                        account_index,
                        storage_index,
                        access_info,
                    })
                } else if let Some(block_info) = self.current_block.as_mut() {
                    let system_account_access_map = match access_context {
                        AccountAccessContext::BlockPrologue => {
                            &mut block_info.prologue_account_accesses
                        }
                        AccountAccessContext::BlockEpilogue => {
                            &mut block_info.epilogue_account_accesses
                        }
                        _ => panic!("unexpected account access context {access_context:?}"),
                    };
                    merge_storage_access_event(system_account_access_map, &access_info);
                    None
                } else {
                    Some(BlockUpdate::OrphanedEvent(ExecEvent::StorageAccess {
                        access_context,
                        account_index,
                        storage_index,
                        access_info,
                    }))
                }
            }

            ExecEvent::EvmError {
                domain_id,
                status_code,
                txn_id,
            } => self.abort_block(BlockExecutionFailure::ExecutionDaemonInternal {
                txn_id,
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

    fn append_txn_event(&mut self, event: ExecEvent) -> Option<BlockUpdate> {
        let block_info = match self.current_block.as_mut() {
            Some(block_info) => block_info,
            None => return Some(BlockUpdate::OrphanedEvent(event)),
        };

        let txn_index = *match &event {
            ExecEvent::TransactionStart { txn_index, .. } => txn_index,
            ExecEvent::TransactionReceipt { txn_index, .. } => txn_index,
            ExecEvent::TransactionLog { txn_index, .. } => txn_index,
            ExecEvent::TransactionCallFrame { txn_index, .. } => txn_index,
            ExecEvent::AccountAccessListHeader {
                access_context: AccountAccessContext::Transaction(txn_index),
                ..
            } => txn_index,
            ExecEvent::AccountAccess {
                access_context: AccountAccessContext::Transaction(txn_index),
                ..
            } => txn_index,
            ExecEvent::StorageAccess {
                access_context: AccountAccessContext::Transaction(txn_index),
                ..
            } => txn_index,
            _ => panic!("should not be called with {event:?}"),
        };

        let txn_info = match block_info.transactions[txn_index as usize].as_mut() {
            Some(txn) => txn,
            None => {
                return Some(BlockUpdate::ImplicitDrop {
                    dropped_block: self.current_block.take().unwrap(),
                    reassembly_error: ReassemblyError::MissingTransaction { txn_index },
                })
            }
        };

        let txn_reassembly_state = match self.current_reassembly_state.as_mut() {
            Some(reassembly) => &mut reassembly.transactions[txn_index as usize],
            None => panic!("no active reassembly state for txn: {txn_info:?}"),
        };

        match event {
            ExecEvent::TransactionReceipt {
                txn_index: _,
                status,
                log_count,
                call_frame_count,
                txn_gas_used,
            } => {
                txn_info.receipt.status = status;
                txn_info.receipt.logs.reserve(log_count);
                txn_info.call_frames.reserve(call_frame_count);
                // We can't compute cumulative gas used until the block
                // completes, since transactions may be recorded
                // out-of-order, even though they're committed in order
                txn_info.txn_gas_used = txn_gas_used;
                txn_reassembly_state.expected_log_count = log_count;
                txn_reassembly_state.expected_call_frame_count = call_frame_count;
            }

            ExecEvent::TransactionLog {
                txn_index: _,
                log_index: _,
                log,
            } => txn_info.receipt.logs.push(log),

            ExecEvent::TransactionCallFrame {
                txn_index: _,
                call_frame_index: _,
                call_frame,
            } => txn_info.call_frames.push(call_frame),

            ExecEvent::AccountAccessListHeader {
                access_context,
                entry_count,
            } => {
                assert!(matches!(
                    access_context,
                    AccountAccessContext::Transaction(_)
                ));
                std::mem::swap(
                    &mut txn_reassembly_state.account_reassembly_state,
                    &mut vec![AccountReassemblyState::default(); entry_count as usize],
                );
                txn_info.account_accesses.reserve(entry_count as usize);
            }

            ExecEvent::AccountAccess {
                access_context: _,
                account_index,
                access_info,
            } => {
                let account_reassembly_state =
                    &mut txn_reassembly_state.account_reassembly_state[account_index as usize];
                account_reassembly_state.address = access_info.address;
                account_reassembly_state.storage_keys_count = access_info.storage_key_count;
                account_reassembly_state.transient_keys_count = access_info.transient_key_count;
                txn_info
                    .account_accesses
                    .insert(access_info.address, make_account_access_info(access_info));
            }

            ExecEvent::StorageAccess {
                access_context,
                account_index: _,
                storage_index: _,
                access_info,
            } => {
                assert!(matches!(
                    access_context,
                    AccountAccessContext::Transaction(_)
                ));
                merge_storage_access_event(&mut txn_info.account_accesses, &access_info);
            }

            _ => panic!("unexpected event: {event:?}"),
        };

        None
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

fn merge_storage_access_event(
    account_access_map: &mut HashMap<Address, AccountAccessInfo>,
    storage_access: &StorageAccess,
) {
    // We ignore the None case, where we're missing the address for some reason
    // (e.g., an undiagnosed missing event). This will be checked for during final
    // block validation
    if let Some(info) = account_access_map.get_mut(&storage_access.address) {
        let storage_map: &mut HashMap<B256, StorageSlot> = if storage_access.is_transient {
            &mut info.transient_accesses
        } else {
            &mut info.storage_accesses
        };
        match storage_map.entry(storage_access.key) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(StorageSlot {
                    original_value: storage_access.original_value,
                    modified_value: storage_access.modified_value,
                });
            }
            hash_map::Entry::Occupied(mut entry) => {
                if let Some(v) = storage_access.modified_value {
                    entry.get_mut().modified_value = Some(v);
                }
            }
        }
    }
}

fn merge_storage_accesses(
    existing_storage_map: &mut HashMap<B256, StorageSlot>,
    new_storage_map: &HashMap<B256, StorageSlot>,
) {
    for (key, new_slot) in new_storage_map {
        match existing_storage_map.entry(*key) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(new_slot.clone());
            }
            hash_map::Entry::Occupied(mut entry) => {
                if new_slot.modified_value.is_some()
                    && new_slot.modified_value != entry.get().modified_value
                {
                    entry.get_mut().modified_value = new_slot.modified_value;
                }
            }
        }
    }
}

fn merge_account_states(
    access_context: AccountAccessContext,
    expected_account_summaries: &[AccountReassemblyState],
    input_map: &HashMap<Address, AccountAccessInfo>,
    merge_map: &mut HashMap<Address, AccountAccessInfo>,
) -> Option<ReassemblyError> {
    if expected_account_summaries.len() != input_map.len() {
        return Some(ReassemblyError::MissingArrayElement {
            location: ArrayElementLocation::Account { access_context },
            expected_element_count: expected_account_summaries.len(),
            actual_element_count: input_map.len(),
        });
    }

    for summary in expected_account_summaries {
        // The name `new_access_info` implies that the input_map contains later
        // account access records than what's already in `merge_map`, replacing
        // state changes if they conflict
        let address = &summary.address;
        let new_access_info = match input_map.get(address) {
            // Given that expected_account_summaries.len() == input_map.len(),
            // this is almost certainly an internal error, not a failure to use
            // the API correctly
            None => panic!(
                "address {address} is missing from map; summary: {summary:?}, map: {input_map:?}"
            ),
            Some(a) => a,
        };

        if summary.storage_keys_count as usize != new_access_info.storage_accesses.len() {
            return Some(ReassemblyError::MissingArrayElement {
                location: ArrayElementLocation::Storage {
                    access_context,
                    address: summary.address,
                },
                expected_element_count: summary.storage_keys_count as usize,
                actual_element_count: new_access_info.storage_accesses.len(),
            });
        }

        if summary.transient_keys_count as usize != new_access_info.transient_accesses.len() {
            return Some(ReassemblyError::MissingArrayElement {
                location: ArrayElementLocation::Storage {
                    access_context,
                    address: summary.address,
                },
                expected_element_count: summary.transient_keys_count as usize,
                actual_element_count: new_access_info.transient_accesses.len(),
            });
        }

        match merge_map.entry(*address) {
            hash_map::Entry::Vacant(entry) => {
                // This account has never been seen before; just clone it
                entry.insert(new_access_info.clone());
            }

            hash_map::Entry::Occupied(mut entry) => {
                // This account was seen in an earlier access; merge the new
                // information into the existing object
                let existing_access_info = entry.get_mut();

                // Replace the updated nonce and balance, but only if there's a
                // newer one. We can't unconditionally assign this; consider if
                // we see the two update sequence `U1:Modify, U2:Read`. The read
                // read update will have a modified_balance of `None`, and we
                // don't want that to clear the earlier modification
                if let Some(new_nonce) = new_access_info.modified_nonce {
                    existing_access_info.modified_nonce = Some(new_nonce);
                }
                if let Some(new_balance) = new_access_info.modified_balance {
                    existing_access_info.modified_balance = Some(new_balance);
                }

                merge_storage_accesses(
                    &mut existing_access_info.storage_accesses,
                    &new_access_info.storage_accesses,
                );
                merge_storage_accesses(
                    &mut existing_access_info.transient_accesses,
                    &new_access_info.transient_accesses,
                );
            }
        }
    }

    None
}

fn try_create_block_update(
    mut exec_info: Box<ExecutedBlockInfo>,
    reassembly: ReassemblyState,
) -> BlockUpdate {
    if let Some(reassembly_error) = merge_account_states(
        AccountAccessContext::BlockPrologue,
        reassembly.prologue_accounts.as_slice(),
        &exec_info.prologue_account_accesses,
        &mut exec_info.all_account_accesses,
    ) {
        return BlockUpdate::ImplicitDrop {
            dropped_block: exec_info,
            reassembly_error,
        };
    }

    let mut cumulative_gas_used = 0;
    for (txn_index, opt_txn_info) in &mut exec_info.transactions.iter_mut().enumerate() {
        let txn_info = match opt_txn_info {
            Some(i) => i,
            None => {
                return BlockUpdate::ImplicitDrop {
                    dropped_block: exec_info,
                    reassembly_error: ReassemblyError::MissingTransaction {
                        txn_index: txn_index as u64,
                    },
                }
            }
        };
        cumulative_gas_used += txn_info.txn_gas_used;
        txn_info.receipt.cumulative_gas_used = cumulative_gas_used;

        let txn_reassembly_state = &reassembly.transactions[txn_index];

        let actual_log_count = txn_info.receipt.logs.len();
        let expected_log_count = txn_reassembly_state.expected_log_count;
        if expected_log_count != actual_log_count {
            return BlockUpdate::ImplicitDrop {
                dropped_block: exec_info,
                reassembly_error: ReassemblyError::MissingArrayElement {
                    location: ArrayElementLocation::Log {
                        txn_index: txn_index as u64,
                    },
                    expected_element_count: expected_log_count,
                    actual_element_count: actual_log_count,
                },
            };
        }

        let actual_call_frame_count = txn_info.call_frames.len();
        let expected_call_frame_count = txn_reassembly_state.expected_call_frame_count;
        if expected_call_frame_count != actual_call_frame_count {
            return BlockUpdate::ImplicitDrop {
                dropped_block: exec_info,
                reassembly_error: ReassemblyError::MissingArrayElement {
                    location: ArrayElementLocation::CallFrame {
                        txn_index: txn_index as u64,
                    },
                    expected_element_count: expected_call_frame_count,
                    actual_element_count: actual_call_frame_count,
                },
            };
        }

        if let Some(reassembly_error) = merge_account_states(
            AccountAccessContext::Transaction(txn_index as u64),
            txn_reassembly_state.account_reassembly_state.as_slice(),
            &txn_info.account_accesses,
            &mut exec_info.all_account_accesses,
        ) {
            return BlockUpdate::ImplicitDrop {
                dropped_block: exec_info,
                reassembly_error,
            };
        }
    }

    if let Some(reassembly_error) = merge_account_states(
        AccountAccessContext::BlockEpilogue,
        reassembly.epilogue_accounts.as_slice(),
        &exec_info.epilogue_account_accesses,
        &mut exec_info.all_account_accesses,
    ) {
        return BlockUpdate::ImplicitDrop {
            dropped_block: exec_info,
            reassembly_error,
        };
    }

    BlockUpdate::Executed(exec_info)
}

fn make_account_access_info(access_record: AccountAccess) -> AccountAccessInfo {
    AccountAccessInfo {
        original_nonce: access_record.original_nonce,
        modified_nonce: access_record.modified_nonce,
        original_balance: access_record.original_balance,
        modified_balance: access_record.modified_balance,
        code_hash: access_record.code_hash,
        storage_accesses: HashMap::with_capacity(access_record.storage_key_count as usize),
        transient_accesses: HashMap::with_capacity(access_record.transient_key_count as usize),
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
