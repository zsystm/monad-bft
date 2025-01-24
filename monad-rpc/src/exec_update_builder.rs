//! This module provides a utility to reassemble low-level, fine-grained
//! real time events from the execution daemon's event streaming service
//! into "block-oriented" updates. That is, all execution events pertaining
//! to a single block (e.g., all emitted logs) are aggregated and returned
//! as part of a single "block update" object.

use std::collections::{HashMap, VecDeque};

use alloy_consensus::{Header, Receipt, TxEip1559, TxEip2930, TxEnvelope, TxLegacy, TxType};
use alloy_eips::eip2930::AccessList;
use alloy_primitives::{
    Address, BlockHash, Bloom, Bytes, PrimitiveSignature, TxKind, B256, B64, U256,
};
use monad_crypto::hasher::Hash;
use monad_event_ring::{
    eth_c_types,
    event_reader::*,
    event_ring::{monad_event_descriptor, *},
    event_ring_util::*,
    exec_event_types,
};
use monad_types::BlockId;

/// Specifies what state a block is in when its information is reported
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum BlockConsensusState {
    Proposed,
    Voted,
    Finalized,
    Verified,
    Abandoned,
}

/// Low-level events from the execution daemon are aggregated into
/// block-at-a-time updates. When all events pertaining to a block's
/// execution have been encountered (or if an error occurs before that
/// happens), one of these "block update" objects is produced.
///
/// Post-execution consensus decisions are also communicated using this
/// same update type: we are told when a block enters the "voted" state
/// (has a QC), when it either becomes finalized or abandoned, and
/// when its state root is verified.
///
/// Blocks can be buffered up until they reach the desired consensus
/// state, to avoid seeing a block too early in the lifecycle
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum BlockUpdate {
    /// Execution of a block proposal failed; no other updates for this BFT
    /// block ID will be delivered
    Failed(Box<FailedBlockInfo>),

    /// Execution of a block succeeded; the block reached the specified
    /// consensus state before being reported. If the consensus state is
    /// not Verified, further ConsensusStateChanged updates will follow
    /// until the block either becomes Verified or Abandoned
    Executed {
        consensus_state: BlockConsensusState,
        exec_info: Box<ExecutedBlockInfo>,
    },

    /// Block with the given BFT block ID (previously seen in an
    /// Executed-variant update) has changed to the specified state as
    /// a result of further rounds in the consensus algorithm
    ConsensusStateChanged {
        new_state: BlockConsensusState,
        bft_block_id: BlockId,
        block_number: u64,
        has_untracked_proposal: bool,
    },
}

/// Collection of all event information aggregated during the execution
/// of a single block
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExecutedBlockInfo {
    pub bft_block_id: BlockId,
    pub consensus_seqno: u64,
    pub eth_header: Header,
    pub eth_block_hash: BlockHash,
    pub txns: Vec<Option<TransactionInfo>>,
}

/// All info about each transaction that occurs in a block
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TransactionInfo {
    pub index: usize,
    pub tx_header: TxEnvelope,
    pub sender: Address,
    pub receipt: Receipt,
    pub tx_gas_used: u128,
}

/// Collection of summary information that is available about a failed
/// block execution
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct FailedBlockInfo {
    pub bft_block_id: BlockId,
    pub consensus_seqno: u64,
    pub eth_header: Header,
    pub failure_reason: BlockExecutionFailure,
}

/// Encodes the reason that a block failed to fully execute
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub enum BlockExecutionFailure {
    /// Attempted to execute a block that failed validation; the numerical
    /// code corresponds to a `BlockError` enumeration constant in
    /// `validate_block.hpp` in the execution daemon source code
    RejectedBlock { code: u64 },

    /// Attempted to execute a transaction that failed validation; the
    /// numerical code corresponds to a `TransactionError` enumeration
    /// constant in `validate_transaction.hpp` in the execution daemon
    /// source code
    RejectedTransaction { txn_id: u32, code: u64 },

    /// Domain ID and error code from the execution daemon's error
    /// reporting system
    ExecutionDaemonInternal { txn_id: u32, domain: u64, code: i64 },
}

/// The result of polling an update builder when no block update is ready
/// because of an underlying problem with the event stream
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub enum EventStreamError {
    /// Execution daemon process was terminated; no more updates will be
    /// published
    Disconnected,

    /// An event sequence number gap occurred; in this case, we won't see a
    /// finalization or abandonment update for any pending blocks (see
    /// the `event.md` documentation for an explanation of why this can
    /// occur)
    Gap { last_seqno: u64, cur_seqno: u64 },

    /// Payload memory was overwritten; similar in effect to Gap, but with
    /// different metadata explaining what went wrong
    PayloadExpired {
        payload_offset: u64,
        buffer_window_start: u64,
    },

    /// Unable to represent the payload in our fixed-sized buffer
    PayloadTruncated(u32),

    /// This is a "catch all" generic error indicating that something has
    /// happened at the protocol level that should not be possible in
    /// normal operation, e.g., seeing finalization events that "skip over"
    /// pending block updates (in the sequence number sense). This indicates
    /// either a logic error in the consensus client or in execution, or
    /// more likely, memory corruption somewhere which only makes an error
    /// _appear_ to have happened. Because the latter is the most likely
    /// cause, it is considered a kind of EventStreamError and should be
    /// treated like `Disconnected`: there is no reasonable way to continue.
    /// The difference is we provide a string description of what assumption
    /// was violated, which may be useful for debugging.
    ProtocolError(String),
}

/// Object which captures low-level execution events and builds complete
/// BlockUpdate objects out of them
pub struct BlockUpdateBuilder<'ring> {
    config: BlockUpdateBuilderConfig,
    reader_state: ReaderState<'ring>,
    update_state: UpdateState,
}

/// Result of polling the block update builder for new block updates
#[derive(Clone, Debug)]
pub enum BlockPollResult {
    NotReady,
    Error(EventStreamError),
    Ready(BlockUpdate),
}

/*
 * BlockUpdateBuilder implementation section
 */

impl std::str::FromStr for BlockConsensusState {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "proposed" => Ok(BlockConsensusState::Proposed),
            "voted" => Ok(BlockConsensusState::Voted),
            "finalized" => Ok(BlockConsensusState::Finalized),
            "verified" => Ok(BlockConsensusState::Verified),
            "abandoned" => Ok(BlockConsensusState::Abandoned),
            _ => Err(()),
        }
    }
}

impl PartialOrd for BlockConsensusState {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (BlockConsensusState::Finalized, BlockConsensusState::Abandoned) => None,
            (BlockConsensusState::Abandoned, BlockConsensusState::Finalized) => None,
            (BlockConsensusState::Abandoned, BlockConsensusState::Verified) => None,
            (a, b) => Some((*a as usize).cmp(&(*b as usize))),
        }
    }
}

/// This holds the event reader state and the fixed-sized buffers that
/// the next incoming event is copied into; all of this data is immutable
/// outside of the code that calls reader.try_copy_all()
struct ReaderState<'ring> {
    reader: EventReader<'ring>,
    event: monad_event_descriptor,
    payload_buf: Vec<u8>,
    block_header_table: &'ring [exec_event_types::block_header],
    not_ready_polls: u64,
}

/// This object tracks:
///
///  - Block updates which are being reassembled during a proposal's
///    execution phase (exec_reassembly)
///
///  - Block updates that are already assembled but not yet handed back to
///    the caller (ready_updates)
///
///  - Info on "pending blocks", which track the BFT block IDs for blocks
///    which were successfully executed, but are not yet Verified
///
///  - The last error in the underlying execution event stream. Some errors
///    are recoverable, whereas others are "sink states." A "sink state," in
///    the state machine sense, is a state that can't be transitioned out of
///    once we reach it. Certain kinds of event stream errors are not
///    recoverable, and once they occur, the update polling operation will
///    return this same error forever. Event gaps and payload expirations are
///    not sink states: they are recoverable, but some updates will be lost
struct UpdateState {
    exec_reassembly: [Option<Box<ExecutedBlockInfo>>; 4096],
    ready_updates: VecDeque<BlockUpdate>,
    pending_block_map: HashMap<u64, Vec<PendingBlock>>,
    event_stream_error: Option<EventStreamError>,
}

pub struct BlockUpdateBuilderConfig {
    pub executed_consensus_state: BlockConsensusState,
    pub parse_txn_input: bool,
    pub report_orphaned_consensus_events: bool,
    pub opt_process_exit_monitor: Option<ProcessExitMonitor>,
}

#[derive(Debug, PartialEq, Eq)]
struct PendingBlock {
    bft_block_id: BlockId,
    reassembly_index: usize,
    block_number: u64,
    last_consensus_state: BlockConsensusState,
}

impl<'ring> BlockUpdateBuilder<'ring> {
    pub fn new(
        event_ring: &'ring EventRing,
        reader: EventReader<'ring>,
        config: BlockUpdateBuilderConfig,
    ) -> BlockUpdateBuilder<'ring> {
        const NONE_INIT: Option<Box<ExecutedBlockInfo>> = None;
        let block_header_table = unsafe {
            std::slice::from_raw_parts(
                event_ring.get_context_area() as *const exec_event_types::block_header,
                4096,
            )
        };
        BlockUpdateBuilder {
            config,
            reader_state: ReaderState {
                reader,
                event: unsafe { std::mem::zeroed::<monad_event_descriptor>() },
                payload_buf: vec![0; 1 << 25],
                block_header_table,
                not_ready_polls: 0,
            },
            update_state: UpdateState {
                exec_reassembly: [NONE_INIT; 4096],
                ready_updates: VecDeque::new(),
                pending_block_map: HashMap::new(),
                event_stream_error: None,
            },
        }
    }

    /// A non-blocking call which checks if any block updates are ready;
    /// if so, the first available update is returned
    pub fn poll(&'_ mut self) -> BlockPollResult {
        if let Some(update) = self.update_state.ready_updates.pop_front() {
            // An update is ready immediately; hand it back
            BlockPollResult::Ready(update)
        } else {
            // No update ready; poll for execution events, trying to reassemble
            // them into a complete update. This will eagerly assemble as many
            // updates as can be completed given the number of events that are
            // available immediately. It returns the first update, if one is
            // ready
            self.poll_events()
        }
    }

    fn poll_events(&mut self) -> BlockPollResult {
        if let Some(e) = &self.update_state.event_stream_error {
            return BlockPollResult::Error(e.clone());
        }
        loop {
            let rs = &mut self.reader_state;
            let next_result = rs.reader.try_next(&mut rs.event);
            match next_result {
                NextResult::NotReady => {
                    rs.not_ready_polls += 1;
                    if rs.not_ready_polls & ((1 << 20) - 1) == 0
                        && self.config.opt_process_exit_monitor.is_some()
                        && self
                            .config
                            .opt_process_exit_monitor
                            .as_ref()
                            .unwrap()
                            .has_exited()
                    {
                        Self::free_reassembly_state(&mut self.update_state);
                        self.update_state
                            .event_stream_error
                            .replace(EventStreamError::Disconnected);
                        return BlockPollResult::Error(EventStreamError::Disconnected);
                    }
                    // The only way we exit the reassembly loop is when no new
                    // events are immediately ready; as long as there's a
                    // backlog we'll keep reassembling. Only when we break
                    // from reassembly here will we try to drain the queue of
                    // reassembled events.
                    break;
                }
                NextResult::Gap => {
                    rs.not_ready_polls = 0;
                    Self::free_reassembly_state(&mut self.update_state);

                    // Reset the reader and report the gap
                    return BlockPollResult::Error(EventStreamError::Gap {
                        last_seqno: self.reader_state.reader.read_last_seqno,
                        cur_seqno: self.reader_state.reader.reset(),
                    });
                }
                NextResult::Success => {
                    rs.not_ready_polls = 0;
                    let rs_payload_buf: *mut u8 = rs.payload_buf.as_mut_ptr();

                    // Rebind "rs" as immutable (the processing code does not need to
                    // change it), copy the event payload, and perform the reassembly
                    // step (i.e., add this execution event to a block update)
                    let rs = &self.reader_state;

                    if rs.event.payload_size as usize > rs.payload_buf.len() {
                        Self::free_reassembly_state(&mut self.update_state);
                        return BlockPollResult::Error(EventStreamError::PayloadTruncated(
                            rs.event.payload_size,
                        ));
                    }
                    if let Some(stream_error) = Self::check_payload_expired(rs) {
                        Self::free_reassembly_state(&mut self.update_state);
                        return BlockPollResult::Error(stream_error);
                    }
                    let payload = rs.reader.payload_peek(&rs.event);
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            payload.as_ptr(),
                            rs_payload_buf,
                            payload.len(),
                        );
                    }
                    if let Some(stream_error) = Self::check_payload_expired(rs) {
                        Self::free_reassembly_state(&mut self.update_state);
                        return BlockPollResult::Error(stream_error);
                    }

                    Self::reassemble_block_event(&self.config, rs, &mut self.update_state);

                    // Reassembly can set a stream error
                    if let Some(stream_error) = self.update_state.event_stream_error.take() {
                        Self::free_reassembly_state(&mut self.update_state);
                        match stream_error {
                            p @ EventStreamError::PayloadExpired { .. } => {
                                return BlockPollResult::Error(p)
                            }
                            other => {
                                // Not recoverable; this is a sink state
                                self.update_state.event_stream_error.replace(other.clone());
                                return BlockPollResult::Error(other);
                            }
                        }
                    }
                }
            };
        }

        if let Some(ready) = self.update_state.ready_updates.pop_front() {
            BlockPollResult::Ready(ready)
        } else {
            BlockPollResult::NotReady
        }
    }

    fn check_payload_expired(reader_state: &ReaderState) -> Option<EventStreamError> {
        use std::sync::atomic::Ordering;
        if !reader_state.reader.payload_check(&reader_state.event) {
            Some(EventStreamError::PayloadExpired {
                payload_offset: reader_state.event.payload_buf_offset,
                buffer_window_start: reader_state
                    .reader
                    .buffer_window_start
                    .load(Ordering::Acquire),
            })
        } else {
            None
        }
    }

    // Destroy any in-progress block reassembly and all drop all ready updates
    // and pending block information. This happens in the case of disconnection
    // or gaps, and requires the listener to discard all state.
    fn free_reassembly_state(u: &mut UpdateState) {
        for ru in &mut u.exec_reassembly {
            ru.take();
        }
        u.ready_updates.clear();
        u.pending_block_map.clear();
    }

    fn reassemble_block_event(c: &BlockUpdateBuilderConfig, r: &ReaderState, u: &mut UpdateState) {
        use exec_event_types::exec_event_type::*;
        let exec_event_type = unsafe {
            std::mem::transmute::<u16, exec_event_types::exec_event_type>(r.event.event_type)
        };
        match exec_event_type {
            BLOCK_START => Self::act_on_block_start(r, u),
            BLOCK_END => Self::act_on_block_end(c, r, u),
            BLOCK_VOTED => Self::act_on_block_voted(c, r, u),
            BLOCK_FINALIZED => Self::act_on_block_finalized(c, r, u),
            BLOCK_VERIFIED => Self::act_on_block_verified(c, r, u),
            BLOCK_REJECT => Self::act_on_block_reject(r, u),
            TXN_START => Self::act_on_txn_start(c, r, u),
            TXN_LOG => Self::act_on_txn_log(r, u),
            TXN_RECEIPT => Self::act_on_txn_receipt(r, u),
            TXN_REJECT => Self::act_on_txn_reject(r, u),
            EVM_ERROR => Self::act_on_evm_error(r, u),
            _ => (),
        }
    }

    fn act_on_block_start(reader_state: &ReaderState, update_state: &mut UpdateState) {
        let rs = reader_state;
        let (block_flow_id, _) = get_flow_ids(&rs.event);
        let block_header = &rs.block_header_table[block_flow_id];
        if update_state.exec_reassembly[block_flow_id].is_some() {
            // This should not happen: if the block ID were legitimately
            // replaced, then we should have seen the end of the block first.
            // If we missed the BLOCK_END event because of a gap or a problem
            // with the event reassembly, then we should have called
            // `free_reassembly_state` and cleared out the object we're now
            // seeing. This is some kind of very unexpected bug.
            let reassembly = update_state.exec_reassembly[block_flow_id]
                .as_ref()
                .unwrap();
            let s = format!(
                "block flow ID {block_flow_id} reused, but something is already there: {:#?}",
                reassembly
            );
            update_state
                .event_stream_error
                .replace(EventStreamError::ProtocolError(s));
            return;
        };
        let exec_block_info = ExecutedBlockInfo {
            bft_block_id: BlockId(Hash(block_header.mch_hash.0)),
            consensus_seqno: block_header.consensus_seqno,
            eth_header: create_alloy_block_header(
                &block_header.parent_eth_hash,
                &block_header.exec_input,
            ),
            eth_block_hash: B256::default(),
            txns: vec![None; block_header.exec_input.txn_count as usize],
        };
        _ = update_state.exec_reassembly[block_flow_id].insert(Box::new(exec_block_info));
    }

    fn act_on_block_end(
        config: &BlockUpdateBuilderConfig,
        reader_state: &ReaderState,
        update_state: &mut UpdateState,
    ) {
        let rs = reader_state;
        let (block_flow_id, _) = get_flow_ids(&rs.event);
        if update_state.exec_reassembly[block_flow_id].is_none() {
            // In this case, we are seeing a BLOCK_END without having seen the
            // start of it, which happens when we first join in the middle of
            // execution running, or because of gaps; we just ignore this
            return;
        }
        let block_update = update_state.exec_reassembly[block_flow_id]
            .as_mut()
            .unwrap();
        let r: &exec_event_types::block_result =
            &unsafe { *(rs.payload_buf.as_ptr() as *const exec_event_types::block_result) };
        let eth_exec_output = &r.exec_output;
        block_update.eth_block_hash = r.eth_block_hash;
        block_update.eth_header.logs_bloom = Bloom::from(&eth_exec_output.logs_bloom);
        block_update.eth_header.state_root = eth_exec_output.state_root;
        block_update.eth_header.receipts_root = eth_exec_output.receipts_root;
        block_update.eth_header.gas_used = eth_exec_output.gas_used;

        // Compute cumulative gas used
        let mut cumulative_gas_used: u128 = 0;
        for (tx_num, opt_tx_info) in block_update.txns.iter_mut().enumerate() {
            let eth_block_hash = block_update.eth_block_hash;
            if opt_tx_info.is_none() {
                panic!("transaction {tx_num} in block {eth_block_hash} is corrupt");
            }
            let gas_used: u128 = opt_tx_info.as_ref().unwrap().tx_gas_used;
            cumulative_gas_used += gas_used;
            opt_tx_info.as_mut().unwrap().receipt.cumulative_gas_used = cumulative_gas_used;
        }

        let pending_blocks = update_state
            .pending_block_map
            .entry(block_update.consensus_seqno)
            .or_default();
        if pending_blocks
            .iter()
            .any(|p| p.bft_block_id == block_update.bft_block_id)
        {
            // This is a duplicate proposal; destroy the reassembled block information
            update_state.exec_reassembly[block_flow_id].take();
            return;
        }

        let create_executed_update_now: bool =
            config.executed_consensus_state == BlockConsensusState::Proposed;

        // Create a PendingBlock to track the consensus state
        pending_blocks.push(PendingBlock {
            bft_block_id: block_update.bft_block_id,
            reassembly_index: if create_executed_update_now {
                0
            } else {
                block_flow_id
            },
            block_number: block_update.consensus_seqno,
            last_consensus_state: BlockConsensusState::Proposed,
        });

        // If the user wants to see proposals, create the Executed update
        if create_executed_update_now {
            update_state.ready_updates.push_back(BlockUpdate::Executed {
                consensus_state: BlockConsensusState::Proposed,
                exec_info: update_state.exec_reassembly[block_flow_id].take().unwrap(),
            });
        }
    }

    fn act_on_block_voted(
        config: &BlockUpdateBuilderConfig,
        reader_state: &ReaderState,
        update_state: &mut UpdateState,
    ) {
        let rs = reader_state;
        let voted_info: &exec_event_types::block_voted =
            &unsafe { *(rs.payload_buf.as_ptr() as *const exec_event_types::block_voted) };

        // Search all the pending blocks, looking for the one with the matching
        // BFT block ID.
        let opt_voted_block: Option<&mut PendingBlock> = match update_state
            .pending_block_map
            .get_mut(&voted_info.consensus_seqno)
        {
            None => None,
            Some(pending_blocks) => pending_blocks
                .iter_mut()
                .find(|p| p.bft_block_id.0 .0 == voted_info.mch_hash.0),
        };

        if opt_voted_block.is_none() {
            // We don't have the associated pending block
            Self::try_report_orphaned_consensus_change(
                BlockConsensusState::Voted,
                BlockId(Hash(voted_info.mch_hash.0)),
                voted_info.consensus_seqno,
                config,
                update_state,
            );
            return;
        }

        let voted_block: &mut PendingBlock = opt_voted_block.unwrap();
        if voted_block.last_consensus_state == BlockConsensusState::Voted {
            // We've already sent an update for this QC. This can happen if
            // the block serves as the parent of a proposal that fails, and is
            // then used as the parent of a subsequent proposal. Each time it
            // appears as a QC in a particular round, execution will report it
            // to us again.
            return;
        }

        match Self::update_pending_block_state(
            config,
            BlockConsensusState::Voted,
            voted_block,
            &mut update_state.exec_reassembly,
        ) {
            Ok(opt_u) => {
                if let Some(u) = opt_u {
                    update_state.ready_updates.push_back(u)
                }
            }
            Err(e) => {
                update_state.event_stream_error.replace(e);
            }
        };
    }

    fn act_on_block_finalized(
        config: &BlockUpdateBuilderConfig,
        reader_state: &ReaderState,
        update_state: &mut UpdateState,
    ) {
        let rs = reader_state;
        let finalize_info: &exec_event_types::block_finalized =
            &unsafe { *(rs.payload_buf.as_ptr() as *const exec_event_types::block_finalized) };
        let finalized_block_id = BlockId(Hash(finalize_info.mch_hash.0));

        // Get the list of pending blocks with the associated sequence number
        let mut pending_blocks = match update_state
            .pending_block_map
            .remove(&finalize_info.consensus_seqno)
        {
            None => {
                // No pending blocks for this block number
                Self::try_report_orphaned_consensus_change(
                    BlockConsensusState::Finalized,
                    finalized_block_id,
                    finalize_info.consensus_seqno,
                    config,
                    update_state,
                );
                return;
            }
            Some(v) => v,
        };

        // This finalize event determines what happens to all pending blocks
        // bearing the same consensus sequence number. The pending block with
        // the matching BFT block ID is finalized, all others are abandoned.
        let mut opt_finalized_block = None;

        pending_blocks.sort_by(|lhs, rhs| lhs.bft_block_id.cmp(&rhs.bft_block_id));
        for pb in pending_blocks.into_iter() {
            if pb.bft_block_id == finalized_block_id {
                opt_finalized_block.replace(pb);
            } else {
                update_state
                    .ready_updates
                    .push_back(BlockUpdate::ConsensusStateChanged {
                        new_state: BlockConsensusState::Abandoned,
                        bft_block_id: pb.bft_block_id,
                        block_number: pb.block_number,
                        has_untracked_proposal: false,
                    });
            }
        }
        if let Some(mut pb) = opt_finalized_block {
            match Self::update_pending_block_state(
                config,
                BlockConsensusState::Finalized,
                &mut pb,
                &mut update_state.exec_reassembly,
            ) {
                Ok(opt_u) => {
                    if let Some(u) = opt_u {
                        update_state.ready_updates.push_back(u)
                    }
                }
                Err(e) => {
                    update_state.event_stream_error.replace(e);
                }
            }
            // Put this back, so that Verified processing can find it
            update_state
                .pending_block_map
                .insert(finalize_info.consensus_seqno, vec![pb]);
        } else {
            Self::try_report_orphaned_consensus_change(
                BlockConsensusState::Finalized,
                finalized_block_id,
                finalize_info.consensus_seqno,
                config,
                update_state,
            );
        }
    }

    fn act_on_block_verified(
        config: &BlockUpdateBuilderConfig,
        reader_state: &ReaderState,
        update_state: &mut UpdateState,
    ) {
        let rs = reader_state;
        let verified_info: &exec_event_types::block_verified =
            &unsafe { *(rs.payload_buf.as_ptr() as *const exec_event_types::block_verified) };

        let block_number: u64 = verified_info.consensus_seqno;
        match update_state.pending_block_map.get_mut(&block_number) {
            None => {
                Self::try_report_orphaned_consensus_change(
                    BlockConsensusState::Verified,
                    BlockId(Hash::default()),
                    verified_info.consensus_seqno,
                    config,
                    update_state,
                );
                return;
            }
            Some(pending_blocks) => {
                if pending_blocks.len() != 1 {
                    // Given how we maintain it elsewhere, we should never have
                    // an empty vector in the pending block map
                    assert!(!pending_blocks.is_empty());
                    let s = format!(
                        "pending block number {block_number} with multiple pending candidates: {pending_blocks:#?}");
                    update_state
                        .event_stream_error
                        .replace(EventStreamError::ProtocolError(s));
                    return;
                }
                match Self::update_pending_block_state(
                    config,
                    BlockConsensusState::Verified,
                    &mut pending_blocks[0],
                    &mut update_state.exec_reassembly,
                ) {
                    Ok(opt_u) => {
                        if let Some(u) = opt_u {
                            update_state.ready_updates.push_back(u)
                        }
                    }
                    Err(e) => {
                        update_state.event_stream_error.replace(e);
                    }
                };
            }
        };
        update_state.pending_block_map.remove(&block_number);
    }

    fn act_on_block_reject(reader_state: &ReaderState, update_state: &mut UpdateState) {
        // We don't expect execution to encounter transactions which can cause BLOCK_REJECT in
        // normal operation. Typically, the execution daemon only executes blocks proposed to
        // it by consensus and only valid blocks should be proposed by us. We produce these
        // only for completeness' sake.
        let code: u64 = unsafe { *(reader_state.payload_buf.as_ptr() as *const u64) };
        Self::abort_block(
            reader_state,
            update_state,
            BlockExecutionFailure::RejectedBlock { code },
        );
    }

    fn act_on_txn_start(
        config: &BlockUpdateBuilderConfig,
        reader_state: &ReaderState,
        update_state: &mut UpdateState,
    ) {
        const TXN_START_SIZE: usize = size_of::<exec_event_types::txn_start>();
        let rs = reader_state;
        let (block_flow_id, txn_id) = get_flow_ids(&rs.event);
        if update_state.exec_reassembly[block_flow_id].is_none() {
            return; // Started listening outside a boundary
        }
        let block_exec_info = update_state.exec_reassembly[block_flow_id]
            .as_mut()
            .unwrap();
        let txn_start: &exec_event_types::txn_start =
            &unsafe { *(rs.payload_buf.as_ptr() as *const exec_event_types::txn_start) };
        let input = if config.parse_txn_input {
            let value = Bytes::copy_from_slice(
                &rs.payload_buf[TXN_START_SIZE..rs.event.payload_size as usize],
            );
            if let Some(stream_error) = Self::check_payload_expired(rs) {
                update_state.event_stream_error.replace(stream_error);
                return; // Input expired; can't continue
            }
            value
        } else {
            Bytes::new()
        };
        let txn_no = txn_id - 1;
        block_exec_info.txns[txn_no].replace(TransactionInfo {
            index: txn_no,
            tx_header: create_alloy_tx_envelope(&txn_start.txn_hash, &txn_start.txn_header, input),
            sender: txn_start.sender,
            receipt: Receipt::default(),
            tx_gas_used: 0,
        });
    }

    fn act_on_txn_log(reader_state: &ReaderState, update_state: &mut UpdateState) {
        use alloy_primitives::Log;
        if let Some(txn_info) = Self::try_get_txn(reader_state, update_state) {
            const LOG_HEADER_SIZE: usize = size_of::<exec_event_types::txn_log>();
            const TOPIC_SIZE: usize = size_of::<B256>();
            let payload_base = reader_state.payload_buf.as_ptr();
            let c_log_header: &exec_event_types::txn_log =
                &unsafe { *(payload_base as *const exec_event_types::txn_log) };
            let topics: &[B256] = unsafe {
                let topic_base = payload_base.wrapping_add(LOG_HEADER_SIZE) as *const B256;
                std::slice::from_raw_parts(topic_base, c_log_header.topic_count as usize)
            };
            let topic_length_bytes: usize = TOPIC_SIZE * c_log_header.topic_count as usize;
            let data_slice: &[u8] = unsafe {
                let data_base = payload_base.wrapping_add(LOG_HEADER_SIZE + topic_length_bytes);
                std::slice::from_raw_parts(data_base, c_log_header.data_length as usize)
            };
            let data: Bytes = Bytes::copy_from_slice(data_slice);
            if let Some(stream_error) = Self::check_payload_expired(reader_state) {
                update_state.event_stream_error.replace(stream_error);
                return; // Topics and/or data expired, can't continue
            }
            txn_info.receipt.logs.push(Log::new_unchecked(
                c_log_header.address,
                Vec::from(topics),
                data,
            ));
        }
    }

    fn act_on_txn_receipt(reader_state: &ReaderState, update_state: &mut UpdateState) {
        if let Some(txn_info) = Self::try_get_txn(reader_state, update_state) {
            let c_receipt: &exec_event_types::txn_receipt = &unsafe {
                *(reader_state.payload_buf.as_ptr() as *const exec_event_types::txn_receipt)
            };
            txn_info.receipt.status = alloy_consensus::Eip658Value::Eip658(c_receipt.status);
            // Ethereum transaction receipts contain the cumulative gas used [YP 4.4.1,
            // Transaction Receipt] whereas TXN_RECEIPT events reports the gas used by that
            // single transaction. Although it's rare, transactions may be recorded out of order,
            // so we can't safely compute cumulative gas until the block is finished.
            txn_info.tx_gas_used = c_receipt.gas_used as u128;
        }
    }

    fn act_on_txn_reject(reader_state: &ReaderState, update_state: &mut UpdateState) {
        // Similar to act_on_block_reject, but for transactions
        let code: u64 = unsafe { *(reader_state.payload_buf.as_ptr() as *const u64) };
        let (_, txn_id) = get_flow_ids(&reader_state.event);
        Self::abort_block(
            reader_state,
            update_state,
            BlockExecutionFailure::RejectedTransaction {
                txn_id: txn_id as u32,
                code,
            },
        );
    }

    fn act_on_evm_error(reader_state: &ReaderState, update_state: &mut UpdateState) {
        // This does the same thing as act_on_*_reject, but for a different
        // reason: EVM_ERROR means that execution failed for a reason other
        // than a validation failure, i.e., the execution daemon experienced
        // some kind of fundamental error (e.g., out of memory).
        let exec_error: &exec_event_types::evm_error =
            &unsafe { *(reader_state.payload_buf.as_ptr() as *const exec_event_types::evm_error) };
        let (_, txn_id) = get_flow_ids(&reader_state.event);
        Self::abort_block(
            reader_state,
            update_state,
            BlockExecutionFailure::ExecutionDaemonInternal {
                txn_id: txn_id as u32,
                domain: exec_error.domain_id,
                code: exec_error.status_code,
            },
        );
    }

    fn try_get_block<'a>(
        reader_state: &ReaderState,
        update_state: &'a mut UpdateState,
    ) -> Option<&'a mut Box<ExecutedBlockInfo>> {
        let (block_flow_id, _) = get_flow_ids(&reader_state.event);
        // In the None case, we are seeing a block-related event without
        // having first seen the start of the block. This happens when we
        // first join in the middle of execution running, or because of
        // gaps; we ignore these events
        update_state.exec_reassembly[block_flow_id].as_mut()
    }

    fn abort_block(
        reader_state: &ReaderState,
        update_state: &mut UpdateState,
        failure_reason: BlockExecutionFailure,
    ) {
        let (block_flow_id, _) = get_flow_ids(&reader_state.event);
        if let Some(exec_info) = update_state.exec_reassembly[block_flow_id].take() {
            let error_info = FailedBlockInfo {
                bft_block_id: exec_info.bft_block_id,
                consensus_seqno: exec_info.consensus_seqno,
                eth_header: exec_info.eth_header.clone(),
                failure_reason,
            };
            update_state
                .ready_updates
                .push_back(BlockUpdate::Failed(Box::new(error_info)));
        }
    }

    fn try_get_txn<'a>(
        reader_state: &ReaderState,
        update_state: &'a mut UpdateState,
    ) -> Option<&'a mut TransactionInfo> {
        if let Some(block_exec_info) = Self::try_get_block(reader_state, update_state) {
            let txn_id = get_flow_ids(&reader_state.event).1;
            if txn_id > 0 {
                block_exec_info.txns[txn_id - 1].as_mut()
            } else {
                None
            }
        } else {
            None
        }
    }

    fn try_report_orphaned_consensus_change(
        new_state: BlockConsensusState,
        bft_block_id: BlockId,
        block_number: u64,
        config: &BlockUpdateBuilderConfig,
        update_state: &mut UpdateState,
    ) {
        // No such block exists; report it anyway, if we're configured to do so
        if config.report_orphaned_consensus_events {
            update_state
                .ready_updates
                .push_back(BlockUpdate::ConsensusStateChanged {
                    new_state,
                    bft_block_id,
                    block_number,
                    has_untracked_proposal: true,
                });
        }
    }

    fn update_pending_block_state(
        config: &BlockUpdateBuilderConfig,
        new_state: BlockConsensusState,
        pending_block: &mut PendingBlock,
        exec_reassembly: &mut [Option<Box<ExecutedBlockInfo>>; 4096],
    ) -> Result<Option<BlockUpdate>, EventStreamError> {
        pending_block.last_consensus_state = new_state;
        if config.executed_consensus_state == new_state {
            let exec_info = exec_reassembly[pending_block.reassembly_index]
                .take()
                .unwrap();
            if exec_info.bft_block_id != pending_block.bft_block_id {
                // TODO(ken): this is a gap-like error, so it shouldn't push
                //    us into a sink state. We don't expect it to ever happen
                //    (we would formally gap first) so we don't care.
                let err = format!(
                    "pending block {:?} [{}] replaced in flow id {} with new block {:?} [{}]",
                    pending_block.bft_block_id,
                    pending_block.block_number,
                    pending_block.reassembly_index,
                    exec_info.bft_block_id,
                    exec_info.eth_header.number
                );
                return Err(EventStreamError::ProtocolError(err));
            }
            return Ok(Some(BlockUpdate::Executed {
                consensus_state: config.executed_consensus_state,
                exec_info,
            }));
        }

        if let Some(std::cmp::Ordering::Less) =
            new_state.partial_cmp(&config.executed_consensus_state)
        {
            // The new state is earlier than we're supposed to inform the
            // user of it happening; we won't create a ConsensusStateChanged
            return Ok(None);
        }

        Ok(Some(BlockUpdate::ConsensusStateChanged {
            new_state,
            bft_block_id: pending_block.bft_block_id,
            block_number: pending_block.block_number,
            has_untracked_proposal: false,
        }))
    }
}

fn create_alloy_tx_envelope(
    txn_hash: &B256,
    txn_header: &eth_c_types::eth_txn_header,
    input: Bytes,
) -> TxEnvelope {
    use alloy_consensus::Signed;
    let tx_type =
        alloy_consensus::TxType::try_from(txn_header.txn_type as u8).expect("bad transaction type");
    let signature = PrimitiveSignature::from_scalars_and_parity(
        B256::from(txn_header.r),
        B256::from(txn_header.s),
        txn_header.y_parity,
    );
    match tx_type {
        TxType::Legacy => TxEnvelope::Legacy(Signed::new_unchecked(
            create_tx_legacy(txn_header, input),
            signature,
            *txn_hash,
        )),
        TxType::Eip1559 => TxEnvelope::Eip1559(Signed::new_unchecked(
            create_tx_eip1559(txn_header, input),
            signature,
            *txn_hash,
        )),
        TxType::Eip2930 => TxEnvelope::Eip2930(Signed::new_unchecked(
            create_tx_eip2930(txn_header, input),
            signature,
            *txn_hash,
        )),
        _ => panic!("transaction type is not supported!"),
    }
}

fn create_tx_legacy(txn_header: &eth_c_types::eth_txn_header, input: Bytes) -> TxLegacy {
    TxLegacy {
        chain_id: match txn_header.chain_id {
            U256::ZERO => None,
            _ => Some(txn_header.chain_id.to::<alloy_primitives::ChainId>()),
        },
        nonce: txn_header.nonce,
        gas_price: txn_header.max_fee_per_gas.to::<u128>(),
        gas_limit: txn_header.gas_limit,
        to: match txn_header.to {
            Address::ZERO => TxKind::Create,
            _ => TxKind::Call(txn_header.to),
        },
        value: txn_header.value,
        input,
    }
}

fn create_tx_eip1559(txn_header: &eth_c_types::eth_txn_header, input: Bytes) -> TxEip1559 {
    TxEip1559 {
        chain_id: txn_header.chain_id.to::<alloy_primitives::ChainId>(),
        nonce: txn_header.nonce,
        gas_limit: txn_header.gas_limit,
        max_fee_per_gas: txn_header.max_fee_per_gas.to::<u128>(),
        max_priority_fee_per_gas: txn_header.max_priority_fee_per_gas.to::<u128>(),
        to: match txn_header.to {
            Address::ZERO => TxKind::Create,
            _ => TxKind::Call(txn_header.to),
        },
        value: txn_header.value,
        access_list: AccessList::default(),
        input,
    }
}

fn create_tx_eip2930(txn_header: &eth_c_types::eth_txn_header, input: Bytes) -> TxEip2930 {
    TxEip2930 {
        chain_id: txn_header.chain_id.to::<alloy_primitives::ChainId>(),
        nonce: txn_header.nonce,
        gas_price: txn_header.max_fee_per_gas.to::<u128>(),
        gas_limit: txn_header.gas_limit,
        to: match txn_header.to {
            Address::ZERO => TxKind::Create,
            _ => TxKind::Call(txn_header.to),
        },
        value: txn_header.value,
        access_list: AccessList::default(),
        input,
    }
}

fn create_alloy_block_header(
    parent_eth_hash: &B256,
    exec_input: &eth_c_types::eth_block_exec_input,
) -> Header {
    Header {
        parent_hash: *parent_eth_hash,
        ommers_hash: exec_input.ommers_hash,
        beneficiary: exec_input.beneficiary,
        state_root: B256::ZERO,
        transactions_root: exec_input.transactions_root,
        receipts_root: B256::ZERO,
        logs_bloom: Bloom::ZERO,
        difficulty: U256::from(exec_input.difficulty),
        number: exec_input.number,
        gas_limit: exec_input.gas_limit,
        gas_used: 0,
        timestamp: exec_input.timestamp,
        extra_data: Bytes::copy_from_slice(
            &exec_input.extra_data.as_slice()[..exec_input.extra_data_length as usize],
        ),
        mix_hash: exec_input.prev_randao,
        nonce: B64::from(&exec_input.nonce),
        base_fee_per_gas: Some(exec_input.base_fee_per_gas.to::<u64>()),
        withdrawals_root: Some(exec_input.withdrawals_root),
        blob_gas_used: None,
        excess_blob_gas: None,
        parent_beacon_block_root: None,
        requests_hash: None,
        target_blobs_per_block: None,
    }
}

pub fn get_flow_ids(event: &monad_event_descriptor) -> (usize, usize) {
    let flow_info =
        unsafe { std::mem::transmute::<u64, exec_event_types::flow_info>(event.user[0]) };
    (flow_info.block_flow_id as usize, flow_info.txn_id as usize)
}

#[cfg(test)]
mod test {
    use monad_event_ring::{
        event_reader::EventReader, exec_event_types_metadata::EXEC_EVENT_DOMAIN_METADATA,
    };

    use crate::exec_update_builder::*;

    #[test]
    fn basic_test() {
        use monad_event_ring::{event_ring::EventRingType, event_test_util};

        const TEST_SCENARIO: &event_test_util::ExecEventTestScenario =
            &event_test_util::ETHEREUM_MAINNET_30B_15M;
        const UNCOMPRESSED_FILE_SIZE: usize = 1 << 24;

        let ring_snapshot = event_test_util::EventRingSnapshot::load_from_scenario(TEST_SCENARIO);

        let mmap_prot = libc::PROT_READ;
        let mmap_extra_flags = 0;
        let event_ring = match EventRing::mmap_from_fd(
            mmap_prot,
            mmap_extra_flags,
            ring_snapshot.snapshot_fd,
            ring_snapshot.snapshot_off,
            TEST_SCENARIO.name,
        ) {
            Err(e) => panic!("event library error -- {e}"),
            Ok(r) => r,
        };
        let mut event_reader = match EventReader::new(
            &event_ring,
            EventRingType::Exec,
            &EXEC_EVENT_DOMAIN_METADATA.metadata_hash,
        ) {
            Err(e) => panic!("event library error -- {e}"),
            Ok(r) => r,
        };
        event_reader.read_last_seqno = 0;
        let mut update_builder = BlockUpdateBuilder::new(
            &event_ring,
            event_reader,
            BlockUpdateBuilderConfig {
                executed_consensus_state: BlockConsensusState::Proposed,
                parse_txn_input: true,
                report_orphaned_consensus_events: true,
                opt_process_exit_monitor: None,
            },
        );

        let mut actual_block_updates: Vec<BlockUpdate> = Vec::new();
        loop {
            match update_builder.poll() {
                BlockPollResult::Ready(update) => actual_block_updates.push(update),
                BlockPollResult::NotReady => break,
                _ => unreachable!(),
            }
        }

        let file_bits = zstd::bulk::decompress(
            TEST_SCENARIO.expected_block_update_json_zst,
            UNCOMPRESSED_FILE_SIZE,
        );
        assert!(file_bits.is_ok());

        let expected_block_updates: Result<Vec<BlockUpdate>, serde_json::Error> =
            serde_json::from_slice(file_bits.unwrap().as_slice());
        assert!(expected_block_updates.is_ok());
        let mut expected_block_updates = expected_block_updates.unwrap();

        for (expected_update, actual_update) in
            expected_block_updates.iter_mut().zip(&actual_block_updates)
        {
            assert_eq!(*expected_update, *actual_update);
        }
    }
}
