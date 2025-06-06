use std::{sync::Arc, time::Duration};

use itertools::Itertools;
use monad_event_ring::{
    EventNextResult, EventRing, SnapshotEventRing, TypedEventReader, TypedEventRing,
};
use monad_exec_events::{
    BlockBuilderResult, BlockCommitState, ConsensusStateTracker, ConsensusStateTrackerUpdate,
    ExecEventRingType, ExecutedBlock,
};
use monad_types::BlockId;
use tokio::sync::broadcast;
use tracing::warn;

use super::{EventServerClient, EventServerEvent, BROADCAST_CHANNEL_SIZE};
use crate::eth_json_types::MonadNotification;

pub struct EventServer<R>
where
    R: TypedEventRing,
{
    event_ring: R,
    consensus_state_tracker: ConsensusStateTracker,
    broadcast_tx: broadcast::Sender<EventServerEvent>,
}

impl EventServer<EventRing<ExecEventRingType>> {
    pub fn new(event_ring: EventRing<ExecEventRingType>) -> EventServerClient {
        let (broadcast_tx, _) = tokio::sync::broadcast::channel(BROADCAST_CHANNEL_SIZE);

        let this = Self {
            event_ring,
            consensus_state_tracker: ConsensusStateTracker::default(),
            broadcast_tx: broadcast_tx.clone(),
        };

        let handle = tokio::spawn(this.run());

        EventServerClient::new(broadcast_tx, handle)
    }

    async fn run(self) {
        let Self {
            event_ring,
            mut consensus_state_tracker,
            broadcast_tx,
        } = self;

        let mut event_reader = event_ring.create_reader();

        loop {
            let event_descriptor = match event_reader.next() {
                EventNextResult::Gap => {
                    warn!("EventServer event_reader gapped");

                    broadcast_event(&broadcast_tx, EventServerEvent::Gap);
                    consensus_state_tracker.reset();
                    continue;
                }
                EventNextResult::NotReady => {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    continue;
                }
                EventNextResult::Ready(event_descriptor) => event_descriptor,
            };

            let Some(result) = consensus_state_tracker.process_event_descriptor(&event_descriptor)
            else {
                continue;
            };

            match result {
                BlockBuilderResult::Rejected => {
                    unimplemented!();
                }
                BlockBuilderResult::PayloadExpired => {
                    warn!("EventServer consensus state tracker gapped through payload expired");

                    broadcast_event(&broadcast_tx, EventServerEvent::Gap);
                    consensus_state_tracker.reset();
                    continue;
                }
                BlockBuilderResult::ImplicitDrop {
                    block,
                    reassembly_error,
                } => {
                    unreachable!("Implicit drop: {reassembly_error:#?}\n{block:#?}");
                }
                BlockBuilderResult::Ok(ConsensusStateTrackerUpdate {
                    block,
                    state,
                    abandoned,
                }) => handle_update(&broadcast_tx, block, state, abandoned),
            }
        }
    }
}

impl EventServer<SnapshotEventRing<ExecEventRingType>> {
    pub(crate) fn new_for_testing(
        snapshot_event_ring: SnapshotEventRing<ExecEventRingType>,
    ) -> EventServerClient {
        Self::new_for_testing_with_delay(snapshot_event_ring, Duration::from_millis(1))
    }

    pub(crate) fn new_for_testing_with_delay(
        snapshot_event_ring: SnapshotEventRing<ExecEventRingType>,
        delay: Duration,
    ) -> EventServerClient {
        let (broadcast_tx, _) = tokio::sync::broadcast::channel(BROADCAST_CHANNEL_SIZE);

        let this = Self {
            event_ring: snapshot_event_ring,
            consensus_state_tracker: ConsensusStateTracker::default(),
            broadcast_tx: broadcast_tx.clone(),
        };

        let handle = tokio::spawn(this.run_for_testing(delay));

        EventServerClient::new(broadcast_tx, handle)
    }

    async fn run_for_testing(self, delay: Duration) {
        tokio::time::sleep(delay).await;

        let Self {
            event_ring,
            mut consensus_state_tracker,
            broadcast_tx,
        } = self;

        let mut event_reader = event_ring.create_reader();

        while let Some(event_descriptor) = event_reader.next() {
            let Some(result) =
                consensus_state_tracker.process_snapshot_event_descriptor(&event_descriptor)
            else {
                continue;
            };

            match result {
                BlockBuilderResult::Rejected => {
                    unimplemented!();
                }
                BlockBuilderResult::PayloadExpired => {
                    unreachable!("SnapshotEventDescriptor payload cannot expire")
                }
                BlockBuilderResult::ImplicitDrop {
                    block,
                    reassembly_error,
                } => {
                    unreachable!("Implicit drop: {reassembly_error:#?}\n{block:#?}");
                }
                BlockBuilderResult::Ok(ConsensusStateTrackerUpdate {
                    block,
                    state,
                    abandoned,
                }) => handle_update(&broadcast_tx, block, state, abandoned),
            }
        }
    }
}

fn handle_update(
    broadcast_tx: &broadcast::Sender<EventServerEvent>,
    block: Arc<ExecutedBlock>,
    commit_state: BlockCommitState,
    abandoned: Option<Vec<Arc<ExecutedBlock>>>,
) {
    if let Some(abandoned) = abandoned {
        for abandoned in abandoned {
            warn!(
                "abandoned [round {}, seqnum {}]",
                abandoned.header.proposal.round, abandoned.header.proposal.block_number
            );
        }
    }

    broadcast_block_updates(&broadcast_tx, block, commit_state);
}

fn broadcast_event(broadcast_tx: &broadcast::Sender<EventServerEvent>, event: EventServerEvent) {
    if let Err(_) = broadcast_tx.send(event) {
        // TODO: The send method only produces an error
        // warn!("EventServer did not send event");
    }
}

fn broadcast_block_updates(
    broadcast_tx: &broadcast::Sender<EventServerEvent>,
    block: Arc<ExecutedBlock>,
    commit_state: BlockCommitState,
) {
    // Given a block update, convert it to a rpc block and logs.
    let block_hash = alloy_primitives::FixedBytes(block.execution_result.eth_block_hash.bytes);
    let block_number = block.header.proposal.block_number;
    let block_timestamp = block.header.exec_input.timestamp;

    let alloy_header = {
        alloy_rpc_types::eth::Header {
            hash: block_hash,
            inner: alloy_consensus::Header {
                parent_hash: alloy_primitives::B256::from(block.header.parent_eth_hash.bytes),
                ommers_hash: alloy_primitives::B256::from(
                    block.header.exec_input.ommers_hash.bytes,
                ),
                beneficiary: alloy_primitives::Address::from(
                    block.header.exec_input.beneficiary.bytes,
                ),
                state_root: alloy_primitives::B256::from(
                    block.execution_result.exec_output.state_root.bytes,
                ),
                transactions_root: alloy_primitives::B256::from(
                    block.header.exec_input.transactions_root.bytes,
                ),
                receipts_root: alloy_primitives::B256::from(
                    block.execution_result.exec_output.receipts_root.bytes,
                ),
                logs_bloom: alloy_primitives::Bloom::from(
                    block.execution_result.exec_output.logs_bloom.bytes,
                ),
                difficulty: alloy_primitives::U256::from(block.header.exec_input.difficulty),
                number: block.header.exec_input.number,
                gas_limit: block.header.exec_input.gas_limit,
                gas_used: block.execution_result.exec_output.gas_used,
                timestamp: block_timestamp,
                extra_data: alloy_primitives::Bytes::from(block.header.exec_input.extra_data.bytes),
                mix_hash: alloy_primitives::B256::from(block.header.exec_input.prev_randao.bytes),
                nonce: alloy_primitives::B64::from(block.header.exec_input.nonce.bytes),
                base_fee_per_gas: alloy_primitives::U256::from_limbs(
                    block.header.exec_input.base_fee_per_gas.limbs,
                )
                .try_into()
                .ok(),
                withdrawals_root: Some(alloy_primitives::B256::from(
                    block.header.exec_input.withdrawals_root.bytes,
                )),
                blob_gas_used: None,
                excess_blob_gas: None,
                parent_beacon_block_root: None,
                requests_hash: None,
                target_blobs_per_block: None,
            },
            total_difficulty: Some(alloy_primitives::U256::ZERO),
            size: None,
        }
    };

    let mut alloy_txns: Vec<alloy_rpc_types::eth::Transaction> = Vec::default();
    let mut alloy_txn_logs: Vec<alloy_rpc_types::Log> = Vec::default();

    let mut txn_log_idx = 0;

    for (txn_index, txn) in block.txns.iter().enumerate() {
        let to = {
            let address = alloy_primitives::Address::from(txn.header.to.bytes);

            if address.is_zero() {
                alloy_primitives::TxKind::Create
            } else {
                alloy_primitives::TxKind::Call(address)
            }
        };

        let txn_signature = alloy_primitives::PrimitiveSignature::from_scalars_and_parity(
            alloy_primitives::B256::from(alloy_primitives::U256::from_limbs(txn.header.r.limbs)),
            alloy_primitives::B256::from(alloy_primitives::U256::from_limbs(txn.header.s.limbs)),
            txn.header.y_parity,
        );

        let txn_hash = alloy_primitives::TxHash::from(txn.hash.bytes);

        let alloy_txn = match txn.header.txn_type {
            monad_exec_events::monad_c_transaction_type_MONAD_TXN_LEGACY => {
                alloy_consensus::TxEnvelope::Legacy(alloy_consensus::Signed::new_unchecked(
                    alloy_consensus::TxLegacy {
                        chain_id: Some(
                            alloy_primitives::U256::from_limbs(txn.header.chain_id.limbs)
                                .try_into()
                                .unwrap(),
                        ),
                        nonce: txn.header.nonce,
                        gas_price: alloy_primitives::U256::from_limbs(
                            txn.header.max_fee_per_gas.limbs,
                        )
                        .try_into()
                        .unwrap(),
                        gas_limit: txn.header.gas_limit,
                        to,
                        value: alloy_primitives::U256::from_limbs(txn.header.value.limbs),
                        input: alloy_primitives::Bytes::copy_from_slice(&txn.input),
                    },
                    txn_signature,
                    txn_hash,
                ))
            }
            monad_exec_events::monad_c_transaction_type_MONAD_TXN_EIP2930 => {
                alloy_consensus::TxEnvelope::Eip2930(alloy_consensus::Signed::new_unchecked(
                    alloy_consensus::TxEip2930 {
                        chain_id: alloy_primitives::U256::from_limbs(txn.header.chain_id.limbs)
                            .try_into()
                            .unwrap(),
                        nonce: txn.header.nonce,
                        gas_price: alloy_primitives::U256::from_limbs(
                            txn.header.max_fee_per_gas.limbs,
                        )
                        .try_into()
                        .unwrap(),
                        gas_limit: txn.header.gas_limit,
                        to,
                        value: alloy_primitives::U256::from_limbs(txn.header.value.limbs),
                        access_list: {
                            assert_eq!(txn.header.access_list_count, 0);
                            alloy_rpc_types::AccessList(Vec::default())
                        },
                        input: alloy_primitives::Bytes::copy_from_slice(&txn.input),
                    },
                    txn_signature,
                    txn_hash,
                ))
            }
            monad_exec_events::monad_c_transaction_type_MONAD_TXN_EIP1559 => {
                alloy_consensus::TxEnvelope::Eip1559(alloy_consensus::Signed::new_unchecked(
                    alloy_consensus::TxEip1559 {
                        chain_id: alloy_primitives::U256::from_limbs(txn.header.chain_id.limbs)
                            .try_into()
                            .unwrap(),
                        nonce: txn.header.nonce,
                        gas_limit: txn.header.gas_limit,
                        max_fee_per_gas: alloy_primitives::U256::from_limbs(
                            txn.header.max_fee_per_gas.limbs,
                        )
                        .try_into()
                        .unwrap(),
                        max_priority_fee_per_gas: alloy_primitives::U256::from_limbs(
                            txn.header.max_priority_fee_per_gas.limbs,
                        )
                        .try_into()
                        .unwrap(),
                        to,
                        value: alloy_primitives::U256::from_limbs(txn.header.value.limbs),
                        access_list: {
                            assert_eq!(txn.header.access_list_count, 0);
                            alloy_rpc_types::AccessList(Vec::default())
                        },
                        input: alloy_primitives::Bytes::copy_from_slice(&txn.input),
                    },
                    txn_signature,
                    txn_hash,
                ))
            }
            _ => panic!(
                "EventServer encountered unknown tx type {}",
                txn.header.txn_type
            ),
        };

        alloy_txns.push(alloy_rpc_types::eth::Transaction {
            inner: alloy_txn,
            block_hash: Some(block_hash),
            block_number: Some(block_number),
            transaction_index: Some(txn_index as u64),
            effective_gas_price: None,
            from: alloy_primitives::Address::from(txn.sender.bytes),
        });

        for txn_log in txn.logs.iter() {
            alloy_txn_logs.push(alloy_rpc_types::Log {
                inner: alloy_primitives::Log {
                    address: alloy_primitives::Address::from(txn_log.address.bytes),
                    data: alloy_primitives::LogData::new_unchecked(
                        txn_log
                            .topic
                            .iter()
                            .chunks(std::mem::size_of::<[u8; 32]>())
                            .into_iter()
                            .map(|topic| {
                                alloy_primitives::B256::from(
                                    TryInto::<[u8; 32]>::try_into(topic.cloned().collect_vec())
                                        .unwrap(),
                                )
                            })
                            .collect(),
                        alloy_primitives::Bytes::copy_from_slice(&txn_log.data),
                    ),
                },
                block_hash: Some(block_hash),
                block_number: Some(block_number),
                block_timestamp: Some(block_timestamp),
                transaction_hash: Some(txn_hash),
                transaction_index: Some(txn_index as u64),
                log_index: Some(txn_log_idx),
                // TODO(andr-dev): Fix this
                removed: false,
            });
            txn_log_idx += 1;
        }
    }

    let block_id = BlockId(monad_types::Hash(block.header.proposal.id.bytes));

    let header = Arc::new(alloy_header);
    let header_speculative = Arc::new(MonadNotification {
        block_id,
        commit_state,
        data: header.clone(),
    });

    let serialized_header = Arc::new(serde_json::to_value(&header).unwrap());
    let serialized_header_speculative =
        Arc::new(serde_json::to_value(&header_speculative).unwrap());

    if commit_state == BlockCommitState::Finalized {
        broadcast_event(
            broadcast_tx,
            EventServerEvent::FinalizedBlock {
                header: header.clone(),
                serialized: serialized_header,
            },
        );
    }

    broadcast_event(
        broadcast_tx,
        EventServerEvent::MonadBlock {
            header_speculative,
            serialized: serialized_header_speculative,
        },
    );

    let logs = alloy_txn_logs.into_iter().map(Arc::new).collect::<Vec<_>>();

    let logs_speculative = Arc::new(
        logs.iter()
            .map(|log| {
                let speculative = MonadNotification {
                    block_id,
                    commit_state,
                    data: log.clone(),
                };
                let speculative_serialized = Arc::new(serde_json::to_value(&speculative).unwrap());

                (speculative, speculative_serialized)
            })
            .collect::<Vec<_>>(),
    );

    if commit_state == BlockCommitState::Finalized {
        let logs = Arc::new(
            logs.into_iter()
                .map(|log| {
                    let serialized = serde_json::to_value(&log).unwrap();

                    (log, Arc::new(serialized))
                })
                .collect::<Vec<_>>(),
        );

        broadcast_event(
            broadcast_tx,
            EventServerEvent::FinalizedLogs {
                header: header.clone(),
                logs: logs.clone(),
            },
        );
    }

    broadcast_event(
        broadcast_tx,
        EventServerEvent::MonadLogs {
            header,
            logs_speculative,
        },
    );
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use monad_event_ring::SnapshotEventRing;

    use crate::event::{EventServer, EventServerEvent};

    #[tokio::test]
    async fn testing_server() {
        let snapshot_event_ring = SnapshotEventRing::new_from_zstd_bytes(
            include_bytes!("../../../monad-exec-events/test/data/exec-events-emn-30b-15m.zst"),
            "TEST",
        )
        .unwrap();

        let event_server_client = EventServer::new_for_testing(snapshot_event_ring);

        let mut subscription = event_server_client.subscribe().unwrap();

        let event = tokio::time::timeout(Duration::from_millis(10), subscription.recv())
            .await
            .unwrap()
            .unwrap();

        match event {
            EventServerEvent::Gap => {
                panic!("EventServer using snapshot should never produce a gap!")
            }
            EventServerEvent::MonadBlock { .. }
            | EventServerEvent::FinalizedBlock { .. }
            | EventServerEvent::MonadLogs { .. }
            | EventServerEvent::FinalizedLogs { .. } => {}
        }
    }
}
