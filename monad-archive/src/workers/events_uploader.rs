use crate::prelude::*;
use std::sync::Arc;

use alloy_consensus::Receipt;
use alloy_primitives::Bloom;
use itertools::Itertools;
use monad_exec_events::{
    BlockBuilder, BlockBuilderResult, BlockCommitState, ConsensusStateTrackerResult,
    ExecEventRingType, ExecEvents, ExecutedBlock,
};
use monad_types::BlockId;

pub fn upload_events(
    event_reader: tokio::sync::mpsc::Receiver<monad_exec_events::ConsensusStateTrackerResult>,
) {
    while let Some(result) = event_reader.recv().await {
        match result {
            ConsensusStateTrackerResult::PayloadExpired => todo!(),
            ConsensusStateTrackerResult::ImplicitDrop {
                block,
                reassembly_error,
            } => todo!(),
            ConsensusStateTrackerResult::Update {
                block,
                state,
                abandoned,
            } => {
                if state != BlockCommitState::Finalized {
                    continue;
                }

                let block_number = block.header.proposal.block_number;
            }
        }
    }
}

fn broadcast_block_updates(
    block: Arc<ExecutedBlock>,
) -> Vec<(
    Header,
    Vec<alloy_rpc_types::eth::Transaction>,
    BlockReceipts,
)> {
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
    let mut alloy_rxs: Vec<ReceiptWithLogIndex> = Vec::default();
    let mut txn_log_idx = 0;

    for (txn_index, txn) in block.txns.iter().enumerate() {
        let mut alloy_txn_logs: Vec<alloy_rpc_types::Log> = Vec::default();

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

        // let receipt = ReceiptWithLogIndex {
        //     receipt: ReceiptEnvelope::,
        //     starting_log_index: todo!(),
        // }
        let mut starting_log_index = txn_log_idx;
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

        let mut cumulative_gas_used = 0_u128;
        let mut call_frames = Vec::with_capacity(txn.call_frames.len());
        for call_frame in txn.call_frames.into_iter() {
            

        }
        let (alloy_txn, alloy_rx) = match txn.header.txn_type {
            monad_exec_events::monad_c_transaction_type_MONAD_TXN_LEGACY => {
                let tx =
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
                    ));
                cumulative_gas_used = cumulative_gas_used + txn.receipt.gas_used as u128;
                let rx = ReceiptWithLogIndex {
                    receipt: ReceiptEnvelope::Legacy(ReceiptWithBloom {
                        receipt: Receipt {
                            status: alloy_consensus::Eip658Value::Eip658(txn.receipt.status),
                            cumulative_gas_used: cumulative_gas_used,
                            logs: alloy_txn_logs.into_iter().map(|log| log.inner).collect(),
                        },
                        logs_bloom: Bloom::new(block.execution_result.exec_output.logs_bloom.bytes),
                    }),
                    starting_log_index,
                };
                (tx, rx)
            } // monad_exec_events::monad_c_transaction_type_MONAD_TXN_EIP2930 => {
              //     alloy_consensus::TxEnvelope::Eip2930(alloy_consensus::Signed::new_unchecked(
              //         alloy_consensus::TxEip2930 {
              //             chain_id: alloy_primitives::U256::from_limbs(txn.header.chain_id.limbs)
              //                 .try_into()
              //                 .unwrap(),
              //             nonce: txn.header.nonce,
              //             gas_price: alloy_primitives::U256::from_limbs(
              //                 txn.header.max_fee_per_gas.limbs,
              //             )
              //             .try_into()
              //             .unwrap(),
              //             gas_limit: txn.header.gas_limit,
              //             to,
              //             value: alloy_primitives::U256::from_limbs(txn.header.value.limbs),
              //             access_list: {
              //                 assert_eq!(txn.header.access_list_count, 0);
              //                 alloy_rpc_types::AccessList(Vec::default())
              //             },
              //             input: alloy_primitives::Bytes::copy_from_slice(&txn.input),
              //         },
              //         txn_signature,
              //         txn_hash,
              //     ))
              // }
              // monad_exec_events::monad_c_transaction_type_MONAD_TXN_EIP1559 => {
              //     alloy_consensus::TxEnvelope::Eip1559(alloy_consensus::Signed::new_unchecked(
              //         alloy_consensus::TxEip1559 {
              //             chain_id: alloy_primitives::U256::from_limbs(txn.header.chain_id.limbs)
              //                 .try_into()
              //                 .unwrap(),
              //             nonce: txn.header.nonce,
              //             gas_limit: txn.header.gas_limit,
              //             max_fee_per_gas: alloy_primitives::U256::from_limbs(
              //                 txn.header.max_fee_per_gas.limbs,
              //             )
              //             .try_into()
              //             .unwrap(),
              //             max_priority_fee_per_gas: alloy_primitives::U256::from_limbs(
              //                 txn.header.max_priority_fee_per_gas.limbs,
              //             )
              //             .try_into()
              //             .unwrap(),
              //             to,
              //             value: alloy_primitives::U256::from_limbs(txn.header.value.limbs),
              //             access_list: {
              //                 assert_eq!(txn.header.access_list_count, 0);
              //                 alloy_rpc_types::AccessList(Vec::default())
              //             },
              //             input: alloy_primitives::Bytes::copy_from_slice(&txn.input),
              //         },
              //         txn_signature,
              //         txn_hash,
              //     ))
              // }
              // _ => panic!(
              //     "EventServer encountered unknown tx type {}",
              //     txn.header.txn_type
              // ),
        };

        alloy_txns.push(alloy_rpc_types::eth::Transaction {
            inner: alloy_txn,
            block_hash: Some(block_hash),
            block_number: Some(block_number),
            transaction_index: Some(txn_index as u64),
            effective_gas_price: None,
            from: alloy_primitives::Address::from(txn.sender.bytes),
        });

        alloy_rxs.push(alloy_rx);

        // for txn_log in txn.logs.iter() {
        //     alloy_txn_logs.push(alloy_rpc_types::Log {
        //         inner: alloy_primitives::Log {
        //             address: alloy_primitives::Address::from(txn_log.address.bytes),
        //             data: alloy_primitives::LogData::new_unchecked(
        //                 txn_log
        //                     .topic
        //                     .iter()
        //                     .chunks(std::mem::size_of::<[u8; 32]>())
        //                     .into_iter()
        //                     .map(|topic| {
        //                         alloy_primitives::B256::from(
        //                             TryInto::<[u8; 32]>::try_into(topic.cloned().collect_vec())
        //                                 .unwrap(),
        //                         )
        //                     })
        //                     .collect(),
        //                 alloy_primitives::Bytes::copy_from_slice(&txn_log.data),
        //             ),
        //         },
        //         block_hash: Some(block_hash),
        //         block_number: Some(block_number),
        //         block_timestamp: Some(block_timestamp),
        //         transaction_hash: Some(txn_hash),
        //         transaction_index: Some(txn_index as u64),
        //         log_index: Some(txn_log_idx),
        //         // TODO(andr-dev): Fix this
        //         removed: false,
        //     });
        //     txn_log_idx += 1;
        // }
    }


    return (alloy_header, alloy_txns, alloy_rxs, block.call_frames);
}
