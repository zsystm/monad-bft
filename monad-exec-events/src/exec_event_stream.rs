//! Module which translates "raw" events in shared memory (which follow the
//! strict, C compatible binary layout rules in `exec_event_ctypes.rs`) into
//! the more ergonomic Rust API types from the `exec_events.rs` module.

use alloy_primitives::{Address, Bytes, TxKind, B256, B64, U256};
use monad_event_ring::{
    event_reader::{self, EventReader},
    event_ring::monad_event_descriptor,
    event_ring_util::ProcessExitMonitor,
};

use crate::{
    eth_ctypes,
    exec_event_ctypes::{self, exec_event_type},
    exec_events::*,
};

/// Result of polling the execution event stream
#[derive(Clone, Debug)]
pub enum PollResult {
    /// No execution event is ready yet
    NotReady,

    /// The execution daemon process is disconnected; this error state is not
    /// recoverable (a new ExecEventStream must be created)
    Disconnected,

    /// Indicates that low-level events were lost in the event ring layer
    /// because events were consumed too slowly. The immediate cause is that
    /// an event sequence number gap occurred (see the `event.md` documentation
    /// for details). When this happens, the event stream has already corrected
    /// the error by skipping over the missing events. This error is returned
    /// to inform the caller that events were lost.
    Gap {
        last_read_seqno: u64,
        last_write_seqno: u64,
    },

    /// Payload memory was overwritten; similar in effect to Gap, but with
    /// different metadata explaining what went wrong
    PayloadExpired {
        expired_seqno: u64,
        last_write_seqno: u64,
        payload_offset: u64,
        buffer_window_start: u64,
    },

    /// A new execution event occurred
    Ready { seqno: u64, event: ExecEvent },
}

// This must be greater than or equal to the maximum consensus execution delay
const FINALIZED_BUFFER_LENGTH: usize = 8;

/// Object which iterates through low-level (C layout compatible) execution
/// events in shared memory, and builds the more ergonomic Rust API type
/// `ExecEvent` to represent them
pub struct ExecEventStream<'ring> {
    reader: EventReader<'ring>,
    config: ExecEventStreamConfig,
    not_ready_polls: u64,
    finalized_buffer: [Option<ProposalMetadata>; FINALIZED_BUFFER_LENGTH],
    finalized_count: usize,
    sink_error: Option<PollResult>,
}

pub struct ExecEventStreamConfig {
    /// If false, the transaction's input is ignored (not made part of the
    /// event); specifically, the alloy_consensus::TxEnvelop field within the
    /// ExecEvent::TransactionStart variant will always be `Bytes::new()`
    pub parse_txn_input: bool,

    /// Optional object used to detect the termination of the execution
    /// daemon; if None, PollResult::Disconnected will not be returned
    pub opt_process_exit_monitor: Option<ProcessExitMonitor>,
}

impl<'ring> ExecEventStream<'ring> {
    pub fn new(
        reader: EventReader<'ring>,
        config: ExecEventStreamConfig,
    ) -> ExecEventStream<'ring> {
        const NONE_INIT: Option<ProposalMetadata> = None;
        ExecEventStream {
            reader,
            config,
            finalized_buffer: [NONE_INIT; FINALIZED_BUFFER_LENGTH],
            finalized_count: 0,
            not_ready_polls: 0,
            sink_error: None,
        }
    }

    /// A non-blocking call which checks if any execution events are ready;
    /// if so, the first available event is consumed and returned
    pub fn poll(&'_ mut self) -> PollResult {
        if let Some(ref x) = self.sink_error {
            return x.clone();
        }
        match self.reader.poll() {
            event_reader::PollResult::NotReady => {
                self.not_ready_polls += 1;
                if self.not_ready_polls & ((1 << 20) - 1) == 0
                    && self.config.opt_process_exit_monitor.is_some()
                    && self
                        .config
                        .opt_process_exit_monitor
                        .as_ref()
                        .unwrap()
                        .has_exited()
                {
                    self.sink_error.replace(PollResult::Disconnected);
                    self.sink_error.as_ref().unwrap().clone()
                } else {
                    PollResult::NotReady
                }
            }
            event_reader::PollResult::Gap {
                last_read_seqno,
                last_write_seqno: _,
            } => {
                self.not_ready_polls = 0;
                self.clear_finalized_buffer();
                // Reset the reader and report the gap
                PollResult::Gap {
                    last_read_seqno,
                    last_write_seqno: self.reader.reset(),
                }
            }
            event_reader::PollResult::Ready(event) => {
                self.not_ready_polls = 0;
                self.create_exec_event(event)
            }
        }
    }

    pub fn reader(&self) -> &EventReader<'ring> {
        &self.reader
    }

    fn create_exec_event(&'_ mut self, event: monad_event_descriptor) -> PollResult {
        use crate::exec_event_ctypes::exec_event_type::*;
        let event_type = unsafe { std::mem::transmute::<u16, exec_event_type>(event.event_type) };
        match event_type {
            BLOCK_START => self.act_on_block_start(event),
            BLOCK_END => self.act_on_block_end(event),
            BLOCK_QC | BLOCK_FINALIZED => self.act_on_consensus_change(event, event_type),
            BLOCK_VERIFIED => self.act_on_block_verified(event),
            BLOCK_REJECT => self.act_on_block_reject(event),
            TXN_START => self.act_on_txn_start(event),
            TXN_REJECT => self.act_on_txn_reject(event),
            TXN_RECEIPT => self.act_on_txn_receipt(event),
            TXN_LOG => self.act_on_txn_log(event),
            TXN_CALL_FRAME => self.act_on_txn_call_frame(event),
            EVM_ERROR => self.act_on_evm_error(event),
            _ => PollResult::NotReady,
        }
    }

    fn act_on_block_start(&'_ mut self, event: monad_event_descriptor) -> PollResult {
        let payload = self.reader.payload_peek(&event);
        let header = unsafe { &*(payload.as_ptr() as *const exec_event_ctypes::block_header) };
        let extra_data = &header.exec_input.extra_data.as_slice()
            [..header.exec_input.extra_data_length as usize];
        self.try_create_event(
            event,
            ExecEvent::BlockStart {
                consensus_state: ConsensusState::Proposed,
                proposal_meta: create_proposal_metadata(&header.proposal),
                chain_id: header.chain_id.to::<u64>(),
                exec_input: EthBlockExecInput {
                    parent_hash: header.parent_eth_hash,
                    ommers_hash: header.exec_input.ommers_hash,
                    beneficiary: header.exec_input.beneficiary,
                    transactions_root: header.exec_input.transactions_root,
                    difficulty: header.exec_input.difficulty,
                    number: header.exec_input.number,
                    gas_limit: header.exec_input.gas_limit,
                    timestamp: header.exec_input.timestamp,
                    extra_data: Bytes::from(extra_data),
                    prev_randao: header.exec_input.prev_randao,
                    nonce: B64::new(header.exec_input.nonce),
                    base_fee_per_gas: match header.exec_input.base_fee_per_gas {
                        U256::ZERO => None,
                        f => Some(f),
                    },
                    withdrawals_root: match header.exec_input.withdrawals_root {
                        B256::ZERO => None,
                        r => Some(r),
                    },
                    transaction_count: header.exec_input.txn_count,
                },
            },
        )
    }

    fn act_on_block_end(&'_ mut self, event: monad_event_descriptor) -> PollResult {
        let payload = self.reader.payload_peek(&event);
        let result = unsafe { &*(payload.as_ptr() as *const exec_event_ctypes::block_result) };
        self.try_create_event(
            event,
            ExecEvent::BlockEnd {
                eth_block_hash: result.eth_block_hash,
                state_root: result.exec_output.state_root,
                receipts_root: result.exec_output.receipts_root,
                logs_bloom: Box::new(alloy_primitives::Bloom::from(result.exec_output.logs_bloom)),
                gas_used: result.exec_output.gas_used,
            },
        )
    }

    fn act_on_block_verified(&'_ mut self, event: monad_event_descriptor) -> PollResult {
        let payload = self.reader.payload_peek(&event);
        let block_number: u64 =
            unsafe { *(payload.as_ptr() as *const exec_event_ctypes::block_verified) }.block_number;
        if let Some(opt_pm) = self.finalized_buffer.iter().find(|opt_pm| {
            opt_pm
                .as_ref()
                .is_some_and(|pp| pp.block_number == block_number)
        }) {
            self.try_create_event(
                event,
                ExecEvent::Referendum {
                    proposal_meta: *opt_pm.as_ref().unwrap(),
                    outcome: ConsensusState::Verified,
                },
            )
        } else {
            PollResult::NotReady
        }
    }

    fn act_on_block_reject(&'_ mut self, event: monad_event_descriptor) -> PollResult {
        let payload = self.reader.payload_peek(&event);
        let reject_code: u32 =
            unsafe { *(payload.as_ptr() as *const exec_event_ctypes::block_reject) };
        self.try_create_event(event, ExecEvent::BlockReject { reject_code })
    }

    fn act_on_consensus_change(
        &'_ mut self,
        event: monad_event_descriptor,
        event_type: exec_event_type,
    ) -> PollResult {
        use crate::exec_event_ctypes::exec_event_type::{BLOCK_FINALIZED, BLOCK_QC};
        let payload = self.reader.payload_peek(&event);
        let proposal_meta =
            unsafe { &*(payload.as_ptr() as *const exec_event_ctypes::proposal_metadata) };
        let poll_result = self.try_create_event(
            event,
            ExecEvent::Referendum {
                proposal_meta: create_proposal_metadata(proposal_meta),
                outcome: match event_type {
                    BLOCK_QC => ConsensusState::QC,
                    BLOCK_FINALIZED => ConsensusState::Finalized,
                    _ => panic!("{event_type:?} unexpected"),
                },
            },
        );
        if let PollResult::Ready {
            seqno: _,
            event:
                ExecEvent::Referendum {
                    proposal_meta,
                    outcome: _,
                },
        } = &poll_result
        {
            if event.event_type == BLOCK_FINALIZED as u16 {
                self.finalized_buffer[self.finalized_count % FINALIZED_BUFFER_LENGTH]
                    .replace(*proposal_meta);
                self.finalized_count += 1;
            }
        };
        poll_result
    }

    fn act_on_txn_start(&'_ mut self, event: monad_event_descriptor) -> PollResult {
        const TXN_START_SIZE: usize = size_of::<exec_event_ctypes::txn_start>();
        let payload = self.reader.payload_peek(&event);
        let txn_no = get_flow_info(&event).txn_id - 1;
        let txn_start = unsafe { &*(payload.as_ptr() as *const exec_event_ctypes::txn_start) };
        let input = if self.config.parse_txn_input {
            Bytes::copy_from_slice(&payload[TXN_START_SIZE..event.payload_size as usize])
        } else {
            Bytes::new()
        };
        self.try_create_event(
            event,
            ExecEvent::TransactionStart {
                index: txn_no as alloy_primitives::TxIndex,
                sender: txn_start.sender,
                tx_envelope: create_alloy_tx_envelope(
                    &txn_start.txn_hash,
                    &txn_start.txn_header,
                    input,
                ),
            },
        )
    }

    fn act_on_txn_log(&'_ mut self, event: monad_event_descriptor) -> PollResult {
        use alloy_primitives::Log;
        const LOG_HEADER_SIZE: usize = size_of::<exec_event_ctypes::txn_log>();
        const TOPIC_SIZE: usize = size_of::<B256>();
        let payload = self.reader.payload_peek(&event);
        let payload_base: *const u8 = payload.as_ptr();
        let txn_no = get_flow_info(&event).txn_id - 1;
        let c_log_header = unsafe { &*(payload.as_ptr() as *const exec_event_ctypes::txn_log) };
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
        self.try_create_event(
            event,
            ExecEvent::TransactionLog {
                index: txn_no as alloy_primitives::TxIndex,
                log: Log::new_unchecked(c_log_header.address, Vec::from(topics), data),
            },
        )
    }

    fn act_on_txn_call_frame(&'_ mut self, event: monad_event_descriptor) -> PollResult {
        const CALL_FRAME_HEADER_SIZE: usize = size_of::<exec_event_ctypes::txn_call_frame>();
        let payload = self.reader.payload_peek(&event);
        let payload_base: *const u8 = payload.as_ptr();
        let txn_no = get_flow_info(&event).txn_id - 1;
        let c_call_frame_header =
            unsafe { &*(payload.as_ptr() as *const exec_event_ctypes::txn_call_frame) };
        let input_slice: &[u8] = unsafe {
            let input_base = payload_base.wrapping_add(CALL_FRAME_HEADER_SIZE);
            std::slice::from_raw_parts(input_base, c_call_frame_header.input_length as usize)
        };
        let input: Bytes = Bytes::copy_from_slice(input_slice);
        let return_slice: &[u8] = unsafe {
            let return_base = payload_base.wrapping_add(CALL_FRAME_HEADER_SIZE + input.len());
            std::slice::from_raw_parts(return_base, c_call_frame_header.return_length as usize)
        };
        let return_value: Bytes = Bytes::copy_from_slice(return_slice);
        self.try_create_event(
            event,
            ExecEvent::TransactionCallFrame {
                index: txn_no as alloy_primitives::TxIndex,
                call_frame: CallFrame {
                    opcode: c_call_frame_header.opcode,
                    caller: c_call_frame_header.caller,
                    call_target: c_call_frame_header.call_target,
                    value: c_call_frame_header.value,
                    gas: c_call_frame_header.gas,
                    gas_used: c_call_frame_header.gas_used,
                    evmc_status_code: c_call_frame_header.evmc_status,
                    depth: c_call_frame_header.depth,
                    input,
                    return_value,
                },
            },
        )
    }

    fn act_on_txn_receipt(&'_ mut self, event: monad_event_descriptor) -> PollResult {
        let payload = self.reader.payload_peek(&event);
        let c_receipt = unsafe { &*(payload.as_ptr() as *const exec_event_ctypes::txn_receipt) };
        let txn_no = get_flow_info(&event).txn_id - 1;
        self.try_create_event(
            event,
            ExecEvent::TransactionReceipt {
                index: txn_no as u64,
                status: alloy_consensus::Eip658Value::Eip658(c_receipt.status),
                log_count: c_receipt.log_count as usize,
                call_frame_count: c_receipt.call_frame_count as usize,
                tx_gas_used: c_receipt.gas_used as u128,
            },
        )
    }

    fn act_on_txn_reject(&'_ mut self, event: monad_event_descriptor) -> PollResult {
        let payload = self.reader.payload_peek(&event);
        let reject_code: u32 =
            unsafe { *(payload.as_ptr() as *const exec_event_ctypes::txn_reject) };
        let txn_no = get_flow_info(&event).txn_id - 1;
        self.try_create_event(
            event,
            ExecEvent::TransactionReject {
                index: txn_no as u64,
                reject_code,
            },
        )
    }

    fn act_on_evm_error(&'_ mut self, event: monad_event_descriptor) -> PollResult {
        // This does the same thing as act_on_*_reject, but for a different
        // reason: EVM_ERROR means that execution failed for a reason other
        // than a validation failure, i.e., the execution daemon experienced
        // some kind of fundamental error (e.g., out of memory).
        let payload = self.reader.payload_peek(&event);
        let exec_error = unsafe { &*(payload.as_ptr() as *const exec_event_ctypes::evm_error) };
        self.try_create_event(
            event,
            ExecEvent::EvmError {
                domain_id: exec_error.domain_id,
                status_code: exec_error.status_code,
                tx_id: get_flow_info(&event).txn_id as u64,
            },
        )
    }

    fn try_create_event(
        &'_ mut self,
        raw_event: monad_event_descriptor,
        exec_event: ExecEvent,
    ) -> PollResult {
        if self.reader.payload_check(&raw_event) {
            PollResult::Ready {
                seqno: raw_event.seqno,
                event: exec_event,
            }
        } else {
            self.clear_finalized_buffer();
            PollResult::PayloadExpired {
                expired_seqno: self.reader.read_last_seqno,
                last_write_seqno: self.reader.reset(),
                payload_offset: raw_event.payload_buf_offset,
                buffer_window_start: self.reader.get_buffer_window_start(),
            }
        }
    }

    fn clear_finalized_buffer(&'_ mut self) {
        for pm in &mut self.finalized_buffer {
            pm.take();
        }
        self.finalized_count = 0;
    }
}

fn create_proposal_metadata(m: &exec_event_ctypes::proposal_metadata) -> ProposalMetadata {
    ProposalMetadata {
        round: m.round,
        epoch: m.epoch,
        block_number: m.block_number,
        id: MonadBlockId(m.id),
        parent_round: m.parent_round,
        parent_id: MonadBlockId(m.parent_id),
    }
}

fn create_alloy_tx_envelope(
    txn_hash: &B256,
    txn_header: &eth_ctypes::eth_txn_header,
    input: Bytes,
) -> alloy_consensus::TxEnvelope {
    use alloy_consensus::{Signed, TxEnvelope, TxType};
    let tx_type = TxType::try_from(txn_header.txn_type as u8).expect("bad transaction type");
    let signature = alloy_primitives::PrimitiveSignature::from_scalars_and_parity(
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

fn get_flow_info(event: &monad_event_descriptor) -> exec_event_ctypes::flow_info {
    unsafe { std::mem::transmute::<u64, exec_event_ctypes::flow_info>(event.user[0]) }
}

fn create_tx_legacy(
    txn_header: &eth_ctypes::eth_txn_header,
    input: Bytes,
) -> alloy_consensus::TxLegacy {
    alloy_consensus::TxLegacy {
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

fn create_tx_eip1559(
    txn_header: &eth_ctypes::eth_txn_header,
    input: Bytes,
) -> alloy_consensus::TxEip1559 {
    alloy_consensus::TxEip1559 {
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
        access_list: alloy_eips::eip2930::AccessList::default(),
        input,
    }
}

fn create_tx_eip2930(
    txn_header: &eth_ctypes::eth_txn_header,
    input: Bytes,
) -> alloy_consensus::TxEip2930 {
    alloy_consensus::TxEip2930 {
        chain_id: txn_header.chain_id.to::<alloy_primitives::ChainId>(),
        nonce: txn_header.nonce,
        gas_price: txn_header.max_fee_per_gas.to::<u128>(),
        gas_limit: txn_header.gas_limit,
        to: match txn_header.to {
            Address::ZERO => TxKind::Create,
            _ => TxKind::Call(txn_header.to),
        },
        value: txn_header.value,
        access_list: alloy_eips::eip2930::AccessList::default(),
        input,
    }
}
