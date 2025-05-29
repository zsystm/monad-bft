use monad_event_ring::{EventDescriptor, EventDescriptorPayload, SnapshotEventDescriptor};

use crate::{
    ffi::{
        monad_c_address, monad_c_bytes32, monad_c_eth_txn_header, monad_c_eth_txn_receipt,
        monad_exec_block_header, monad_exec_block_result, monad_exec_txn_receipt,
        monad_exec_txn_start,
    },
    ExecEventRingType, ExecEvents, ExecEventsRef,
};

#[derive(Debug)]
pub struct ExecutedBlock {
    pub header: monad_exec_block_header,
    pub execution_result: monad_exec_block_result,
    pub txns: Box<[ExecutedTxn]>,
}

#[derive(Debug)]
pub struct ExecutedTxn {
    pub hash: monad_c_bytes32,
    pub sender: monad_c_address,
    pub header: monad_c_eth_txn_header,
    pub input: Box<[u8]>,
    pub logs: Box<[ExecutedTxnLog]>,
    pub receipt: monad_c_eth_txn_receipt,
}

#[derive(Debug)]
pub struct ExecutedTxnLog {
    pub address: monad_c_address,
    pub topic: Box<[u8]>,
    pub data: Box<[u8]>,
}

struct BlockReassemblyState {
    header: monad_exec_block_header,
    txns: Box<[Option<TxnReassemblyState>]>,
}

pub struct TxnReassemblyState {
    hash: monad_c_bytes32,
    sender: monad_c_address,
    header: monad_c_eth_txn_header,
    input: Box<[u8]>,
    logs: Vec<ExecutedTxnLog>,
    receipt: Option<monad_c_eth_txn_receipt>,
}

pub enum BlockBuilderResult {
    Failed(),

    PayloadExpired,

    // TODO(andr-dev): Can or cannot?
    /// Variant used when a block must be dropped because it can be reassembled
    /// correctly; the user can prevent these from being created by detecting
    /// recoverable event stream errors themselves (gaps, etc.) and calling
    /// "drop_block" to discard the current in-process block; these will never
    /// occur unless events are missing
    ImplicitDrop {
        block: monad_exec_block_header,
        reassembly_error: ReassemblyError,
    },
    ExecutedBlock(ExecutedBlock),
}

/// Error that occurs when we can detect that an event is missing, e.g.,
/// at the end of the block we didn't see all the transactions
#[derive(Clone, Debug)]
pub enum ReassemblyError {
    /// We encountered a new BlockStart before the current block completed;
    /// that new BlockStart is returned, so that it can be retried
    UnterminatedBlock {
        block_start: monad_exec_block_header,
    },

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

#[derive(Default)]
pub struct BlockBuilder {
    state: Option<BlockReassemblyState>,
}

impl BlockBuilder {
    pub fn process_event_descriptor<'ring, 'reader>(
        &mut self,
        event_descriptor: &EventDescriptor<'ring, 'reader, ExecEventRingType>,
    ) -> Option<BlockBuilderResult> {
        match event_descriptor.try_map_payload(Self::select_event_ref) {
            EventDescriptorPayload::Payload(Some(exec_event)) => {
                self.process_exec_event(exec_event)
            }
            EventDescriptorPayload::Payload(None) => None,
            EventDescriptorPayload::Expired => {
                self.reset();

                Some(BlockBuilderResult::PayloadExpired)
            }
        }
    }

    pub fn process_snapshot_event_descriptor<'ring, 'reader>(
        &mut self,
        event_descriptor: &SnapshotEventDescriptor<'ring, 'reader, ExecEventRingType>,
    ) -> Option<BlockBuilderResult> {
        self.process_exec_event(event_descriptor.try_map_payload(Self::select_event_ref)?)
    }

    fn select_event_ref(event_ref: ExecEventsRef<'_>) -> Option<ExecEvents> {
        match event_ref {
            ExecEventsRef::BlockQC(_)
            | ExecEventsRef::BlockFinalized(_)
            | ExecEventsRef::BlockVerified(_)
            | ExecEventsRef::TxnCallFrame { .. }
            | ExecEventsRef::AccountAccessListHeader(_)
            | ExecEventsRef::AccountAccess(_)
            | ExecEventsRef::StorageAccess(_) => None,

            event => Some(event.into_owned()),
        }
    }

    fn process_exec_event(&mut self, exec_event: ExecEvents) -> Option<BlockBuilderResult> {
        match exec_event {
            ExecEvents::BlockQC(_)
            | ExecEvents::BlockFinalized(_)
            | ExecEvents::BlockVerified(_)
            | ExecEvents::TxnCallFrame { .. }
            | ExecEvents::AccountAccessListHeader(_)
            | ExecEvents::AccountAccess(_)
            | ExecEvents::StorageAccess(_) => unreachable!(),
            ExecEvents::BlockStart(block_header) => {
                if let Some(dropped_state) = self.state.take() {
                    return Some(BlockBuilderResult::ImplicitDrop {
                        block: dropped_state.header,
                        reassembly_error: ReassemblyError::UnterminatedBlock {
                            block_start: block_header,
                        },
                    });
                }

                let txn_count = block_header.exec_input.txn_count.try_into().unwrap();

                let mut txns = Vec::with_capacity(txn_count);
                txns.resize_with(txn_count, || None);

                self.state = Some(BlockReassemblyState {
                    header: block_header,
                    txns: txns.into_boxed_slice(),
                });

                None
            }
            ExecEvents::BlockEnd(block_result) => {
                let Some(BlockReassemblyState { header, txns }) = self.state.take() else {
                    return None;
                };

                Some(BlockBuilderResult::ExecutedBlock(ExecutedBlock {
                    header,
                    execution_result: block_result,
                    txns: txns
                        .into_vec()
                        .into_iter()
                        .map(|txn_opt| txn_opt.expect("BlockBuilder received TxnStart for txn"))
                        .map(
                            |TxnReassemblyState {
                                 hash,
                                 sender,
                                 header,
                                 input,
                                 logs,
                                 receipt,
                             }| ExecutedTxn {
                                hash,
                                sender,
                                header,
                                input,
                                logs: logs.into_boxed_slice(),
                                receipt: receipt
                                    .expect("BlockBuilder received TxnReeceipt for txn"),
                            },
                        )
                        .collect(),
                }))
            }
            ExecEvents::BlockReject(block_reject) => {
                return Some(BlockBuilderResult::Failed());
            }
            ExecEvents::TxnStart {
                txn_index: index,
                txn_start,
                data_bytes,
            } => {
                let Some(state) = self.state.as_mut() else {
                    return None;
                };

                let monad_exec_txn_start {
                    txn_hash,
                    sender,
                    txn_header,
                } = txn_start;

                let txn_ref = state
                    .txns
                    .get_mut(TryInto::<usize>::try_into(index).unwrap())
                    .expect("BlockBuilder TxnStart txn_index within bounds");

                assert!(txn_ref.is_none());

                *txn_ref = Some(TxnReassemblyState {
                    hash: txn_hash,
                    sender,
                    header: txn_header,
                    input: data_bytes,
                    logs: Vec::default(),
                    receipt: None,
                });

                None
            }
            ExecEvents::TxnReject {
                txn_index: index,
                reject,
            } => {
                let Some(state) = self.state.as_mut() else {
                    return None;
                };

                todo!();
            }
            ExecEvents::TxnReceipt {
                txn_index: index,
                receipt,
            } => {
                let Some(state) = self.state.as_mut() else {
                    return None;
                };

                let monad_exec_txn_receipt {
                    receipt,
                    call_frame_count,
                } = receipt;

                let txn_ref = state
                    .txns
                    .get_mut(TryInto::<usize>::try_into(index).unwrap())
                    .expect("BlockBuilder TxnReceipt txn_index within bounds")
                    .as_mut()
                    .expect("BlockBuilder TxnReceipt txn_index populated from preceding TxnStart");

                assert!(txn_ref.receipt.is_none());
                // assert_eq!(txn_ref.logs.len(), receipt.log_count as usize);

                txn_ref.receipt = Some(receipt);

                None
            }
            ExecEvents::TxnLog {
                txn_index,
                txn_log,
                topic_bytes,
                data_bytes,
            } => {
                let Some(state) = self.state.as_mut() else {
                    return None;
                };

                let txn_ref = state
                    .txns
                    .get_mut(TryInto::<usize>::try_into(txn_index).unwrap())
                    .expect("BlockBuilder TxnLog txn_index within bounds")
                    .as_mut()
                    .expect("BlockBuilder TxnLog txn_index populated from preceding TxnStart");

                assert_eq!(txn_ref.logs.len(), txn_log.index as usize);

                txn_ref.logs.push(ExecutedTxnLog {
                    address: txn_log.address,
                    topic: topic_bytes,
                    data: data_bytes,
                });

                None
            }
            ExecEvents::EvmError(monad_exec_evm_error) => {
                todo!()
            }
        }
    }

    pub fn reset(&mut self) {
        self.state = None;
    }
}

#[cfg(test)]
mod test {
    use monad_event_ring::{SnapshotEventRing, TypedEventReader, TypedEventRing};

    use crate::{
        block_builder::{BlockBuilder, BlockBuilderResult},
        ExecEventRingType,
    };

    fn run_block_builder(snapshot_name: &'static str, snapshot_zstd_bytes: &'static [u8]) {
        let snapshot = SnapshotEventRing::<ExecEventRingType>::new_from_zstd_bytes(
            snapshot_zstd_bytes,
            snapshot_name,
        )
        .unwrap();

        let mut event_reader = snapshot.create_reader();

        let mut block_builder = BlockBuilder::default();

        while let Some(event_descriptor) = event_reader.next() {
            let Some(result) = block_builder.process_snapshot_event_descriptor(&event_descriptor)
            else {
                continue;
            };

            match result {
                BlockBuilderResult::Failed() => {
                    panic!("failed :(")
                }
                BlockBuilderResult::PayloadExpired => panic!("payload expired on snapshot???"),
                BlockBuilderResult::ImplicitDrop {
                    block,
                    reassembly_error,
                } => {
                    panic!("implicit drop");
                }
                BlockBuilderResult::ExecutedBlock(executed_block) => {
                    eprintln!("{executed_block:#?}");
                }
            }
        }
    }

    #[test]
    fn basic_test_ethereum_mainnet() {
        const SNAPSHOT_NAME: &str = "ETHEREUM_MAINNET_30B_15M";
        const SNAPSHOT_ZSTD_BYTES: &[u8] =
            include_bytes!("../test/data/exec-events-emn-30b-15m.zst");

        run_block_builder(&SNAPSHOT_NAME, &SNAPSHOT_ZSTD_BYTES);
    }

    #[test]
    fn basic_test_monad_testnet() {
        const SNAPSHOT_NAME: &str = "MONAD_DEVNET_21B_GENESIS";
        const SNAPSHOT_ZSTD_BYTES: &[u8] =
            include_bytes!("../test/data/exec-events-mdn-21b-genesis.zst");

        run_block_builder(&SNAPSHOT_NAME, &SNAPSHOT_ZSTD_BYTES);
    }
}
