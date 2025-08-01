// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use alloy_consensus::{transaction::Recovered, SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{hex, Address, TxKind, B256, U256};
use alloy_rlp::Encodable;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use itertools::Itertools;
use monad_consensus_types::{
    block::{BlockPolicy, GENESIS_TIMESTAMP},
    payload::RoundSignature,
};
use monad_crypto::{certificate_signature::CertificateKeyPair, NopKeyPair, NopSignature};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_testutil::{generate_block_with_txs, make_eip1559_tx, make_legacy_tx, recover_tx};
use monad_eth_txpool::{EthTxPool, EthTxPoolEventTracker, EthTxPoolMetrics};
use monad_eth_txpool_types::EthTxPoolSnapshot;
use monad_eth_types::BASE_FEE_PER_GAS;
use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
use monad_testutil::signing::MockSignatures;
use monad_types::{Balance, Round, SeqNum, GENESIS_SEQ_NUM};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tracing_test::traced_test;

const EXECUTION_DELAY: u64 = 4;
const BASE_FEE: u128 = BASE_FEE_PER_GAS as u128;
const GAS_LIMIT: u64 = 30000;
const PROPOSAL_GAS_LIMIT: u64 = 300_000_000;
const PROPOSAL_SIZE_LIMIT: u64 = 4_000_000;

// pubkey starts with AAA
const S1: B256 = B256::new(hex!(
    "0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad"
));

// pubkey starts with BBB
const S2: B256 = B256::new(hex!(
    "009ac901cf45a2e92e7e7bdf167dc52e3a6232be3c56cc3b05622b247c2c716a"
));

// pubkey starts with CCC
const S3: B256 = B256::new(hex!(
    "0d756f31a3e98f1ae46475687cbfe3085ec74b3abdd712decff3e1e5e4c697a2"
));

// pubkey starts with DDD
const S4: B256 = B256::new(hex!(
    "871683e86bef90f2e790e60e4245916c731f540eec4a998697c2cbab4e156868"
));

// pubkey starts with EEE
const S5: B256 = B256::new(hex!(
    "9c82e5ab4dda8da5391393c5eb7cb8b79ca8e03b3028be9ba1e31f2480e17dc8"
));

type SignatureType = NopSignature;
type SignatureCollectionType = MockSignatures<SignatureType>;
type StateBackendType = InMemoryState;

fn make_test_block_policy() -> EthBlockPolicy<SignatureType, SignatureCollectionType> {
    EthBlockPolicy::new(
        GENESIS_SEQ_NUM,
        EXECUTION_DELAY,
        1337,
        100_000_000_000_000_000_000,
    )
}

#[derive(Clone)]
enum TxPoolTestEvent<'a> {
    InsertTxs {
        txs: Vec<(&'a TxEnvelope, bool)>,
        expected_pool_size_change: usize,
    },
    InsertTxBatch {
        txs: Vec<&'a TxEnvelope>,
        should_insert: bool,
    },
    CreateProposal {
        tx_limit: usize,
        gas_limit: u64,
        expected_txs: Vec<&'a TxEnvelope>,
        add_to_blocktree: bool,
    },
    CommitPendingBlocks {
        num_blocks: usize,
        expected_committed_seq_num: u64,
    },
    Block(Arc<dyn Fn(&mut EthTxPool<SignatureType, SignatureCollectionType, StateBackendType>)>),
}

fn run_custom_iter<const N: usize>(
    mut eth_block_policy: EthBlockPolicy<SignatureType, SignatureCollectionType>,
    nonces_override: Option<BTreeMap<Address, u64>>,
    events: [TxPoolTestEvent<'_>; N],
    owned: bool,
) {
    let state_backend = {
        let nonces = if let Some(nonces) = nonces_override {
            nonces
        } else {
            events
                .iter()
                .flat_map(|event| match event {
                    TxPoolTestEvent::InsertTxs {
                        txs,
                        expected_pool_size_change: _,
                    } => txs
                        .iter()
                        .map(|(tx, _)| tx.recover_signer().expect("signer is recoverable"))
                        .collect_vec(),
                    TxPoolTestEvent::InsertTxBatch {
                        txs,
                        should_insert: _,
                    } => txs
                        .into_par_iter()
                        .map(|tx| tx.recover_signer().expect("signer is recoverable"))
                        .collect(),
                    _ => vec![],
                })
                .map(|address| (address, 0))
                .collect()
        };

        InMemoryStateInner::new(Balance::MAX, SeqNum(4), InMemoryBlockState::genesis(nonces))
    };

    let mut pool = EthTxPool::default_testing();
    let metrics = EthTxPoolMetrics::default();
    let mut ipc_events = Vec::default();
    let mut event_tracker = EthTxPoolEventTracker::new(&metrics, &mut ipc_events);

    pool.update_committed_block(
        &mut event_tracker,
        generate_block_with_txs(Round(0), SeqNum(0), Vec::default()),
    );

    let mut current_round = 1u64;
    let mut current_seq_num = 1u64;
    let mut pending_blocks = VecDeque::default();

    for event in events {
        match event {
            TxPoolTestEvent::InsertTxs {
                txs,
                expected_pool_size_change,
            } => {
                let pool_previous_num_txs = pool.num_txs();

                for (tx, should_insert) in txs {
                    let tx = recover_tx(tx.to_owned());

                    let mut was_inserted = false;

                    pool.insert_txs(
                        &mut event_tracker,
                        &eth_block_policy,
                        &state_backend,
                        vec![tx.clone()],
                        owned,
                        |inserted_tx| {
                            assert_eq!(&tx, inserted_tx.raw());

                            if !should_insert {
                                panic!("tx was inserted when it shouldn't have been!");
                            }

                            was_inserted = true;
                        },
                    );

                    if should_insert && !was_inserted {
                        panic!(
                            "tx should have been inserted but was not! last event: {:?}",
                            ipc_events.last()
                        );
                    }
                }

                assert_eq!(
                    pool.num_txs(),
                    pool_previous_num_txs
                        .checked_add(expected_pool_size_change)
                        .expect("pool size change does not overflow"),
                );
            }
            TxPoolTestEvent::InsertTxBatch { txs, should_insert } => {
                let pool_previous_num_txs = pool.num_txs();

                let mut num_inserted = 0;
                let num_expected = should_insert.then_some(txs.len()).unwrap_or_default();

                pool.insert_txs(
                    &mut event_tracker,
                    &eth_block_policy,
                    &state_backend,
                    txs.into_iter()
                        .map(ToOwned::to_owned)
                        .map(recover_tx)
                        .collect(),
                    owned,
                    |_| {
                        if !should_insert {
                            panic!("tx inserted when it shouldn't have been!");
                        }

                        num_inserted += 1;
                    },
                );

                assert_eq!(num_inserted, num_expected, "tx insertion count mismatch");

                assert_eq!(
                    pool.num_txs(),
                    pool_previous_num_txs
                        .checked_add(num_expected)
                        .expect("pool size change does not overflow"),
                    "pool size tx insertion count mismatch"
                );
            }
            TxPoolTestEvent::CreateProposal {
                tx_limit,
                gas_limit,
                expected_txs,
                add_to_blocktree,
            } => {
                let mock_keypair = NopKeyPair::from_bytes(&mut [5_u8; 32]).unwrap();
                let encoded_txns = pool
                    .create_proposal(
                        &mut event_tracker,
                        SeqNum(current_seq_num),
                        tx_limit,
                        gas_limit,
                        PROPOSAL_SIZE_LIMIT,
                        [0_u8; 20],
                        GENESIS_TIMESTAMP + current_seq_num as u128,
                        RoundSignature::new(Round(0), &mock_keypair),
                        pending_blocks.iter().cloned().collect_vec(),
                        &eth_block_policy,
                        &state_backend,
                    )
                    .expect("create proposal succeeds");

                let decoded_txns = encoded_txns.body.transactions;

                let expected_txs = expected_txs.into_iter().cloned().collect_vec();

                assert_eq!(
                    decoded_txns,
                    expected_txs,
                    "create_proposal decoded txns do not match expected txs!\n{:#?}",
                    decoded_txns
                        .iter()
                        .zip_longest(expected_txs.iter())
                        .collect_vec()
                );

                if add_to_blocktree {
                    let block = generate_block_with_txs(
                        Round(current_round),
                        SeqNum(current_seq_num),
                        decoded_txns
                            .into_iter()
                            .map(|tx| {
                                let signer = tx.recover_signer().unwrap();
                                Recovered::new_unchecked(tx, signer)
                            })
                            .collect(),
                    );

                    current_seq_num += 1;

                    pending_blocks.push_back(block);
                }

                current_round += 1;
            }
            TxPoolTestEvent::CommitPendingBlocks {
                num_blocks,
                expected_committed_seq_num,
            } => {
                for _ in 0..num_blocks {
                    let block = pending_blocks
                        .pop_front()
                        .expect("missing block in blocktree");

                    BlockPolicy::<_, _, _, StateBackendType>::update_committed_block(
                        &mut eth_block_policy,
                        &block,
                    );

                    pool.update_committed_block(&mut event_tracker, block);
                }

                assert_eq!(
                    expected_committed_seq_num,
                    eth_block_policy.get_last_commit().0
                );
            }
            TxPoolTestEvent::Block(f) => f(&mut pool),
        }
    }
}

fn run_custom<const N: usize>(
    eth_block_policy_generator: impl Fn() -> EthBlockPolicy<SignatureType, SignatureCollectionType>,
    nonces_override: Option<BTreeMap<Address, u64>>,
    events: [TxPoolTestEvent<'_>; N],
) {
    for owned in [false, true] {
        run_custom_iter(
            eth_block_policy_generator(),
            nonces_override.clone(),
            events.clone(),
            owned,
        );
    }
}

fn run_simple<const N: usize>(events: [TxPoolTestEvent<'_>; N]) {
    run_custom(make_test_block_policy, None, events);
}

#[test]
#[traced_test]
fn test_insert_tx_exceeds_gas_limit() {
    let tx = make_legacy_tx(S1, BASE_FEE, PROPOSAL_GAS_LIMIT + 1, 0, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx, false)],
            expected_pool_size_change: 0,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 1,
            gas_limit: PROPOSAL_GAS_LIMIT,
            expected_txs: vec![],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_create_proposal_with_insufficient_tx_limit() {
    let tx = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 0,
            gas_limit: GAS_LIMIT,
            expected_txs: vec![],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::Block(Arc::new(|pool| {
            assert_eq!(pool.num_txs(), 1);
        })),
    ]);
}

#[test]
#[traced_test]
fn test_create_partial_proposal_with_insufficient_gas_limit() {
    let tx1 = make_legacy_tx(S1, BASE_FEE, PROPOSAL_GAS_LIMIT / 2, 0, 10);
    let tx2 = make_legacy_tx(S1, BASE_FEE, PROPOSAL_GAS_LIMIT / 2, 1, 10);
    let tx3 = make_legacy_tx(S1, BASE_FEE, PROPOSAL_GAS_LIMIT / 2, 2, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true), (&tx2, true), (&tx3, true)],
            expected_pool_size_change: 3,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 3,
            gas_limit: PROPOSAL_GAS_LIMIT,
            expected_txs: vec![&tx1, &tx2],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::Block(Arc::new(|pool| {
            assert_eq!(pool.num_txs(), 3);
        })),
    ]);
}

#[test]
#[traced_test]
fn test_basic_price_priority() {
    let tx1 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_legacy_tx(S2, 2 * BASE_FEE, 2 * GAS_LIMIT, 0, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true), (&tx2, true)],
            expected_pool_size_change: 2,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 2,
            gas_limit: GAS_LIMIT * 3,
            expected_txs: vec![&tx2, &tx1],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::Block(Arc::new(|pool| {
            assert_eq!(pool.num_txs(), 2);
        })),
    ]);
}

#[test]
#[traced_test]
fn test_resubmit_with_same_price() {
    let tx1 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 1000);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true), (&tx2, false)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 2,
            gas_limit: GAS_LIMIT * 2,
            expected_txs: vec![&tx1],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_resubmit_with_better_price() {
    let tx1 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_legacy_tx(S1, 2 * BASE_FEE, 2 * GAS_LIMIT, 0, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true), (&tx2, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 2,
            gas_limit: GAS_LIMIT * 3,
            expected_txs: vec![&tx2],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn nontrivial_example() {
    let tx1 = make_legacy_tx(S1, 10 * BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_legacy_tx(S1, 5 * BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_legacy_tx(S1, 3 * BASE_FEE, GAS_LIMIT, 2, 10);
    let tx4 = make_legacy_tx(S2, 5 * BASE_FEE, GAS_LIMIT, 0, 10);
    let tx5 = make_legacy_tx(S2, 3 * BASE_FEE, GAS_LIMIT, 1, 10);
    let tx6 = make_legacy_tx(S2, 1 * BASE_FEE, GAS_LIMIT, 2, 10);
    let tx7 = make_legacy_tx(S3, 8 * BASE_FEE, GAS_LIMIT, 0, 10);
    let tx8 = make_legacy_tx(S3, 9 * BASE_FEE, GAS_LIMIT, 1, 10);
    let tx9 = make_legacy_tx(S3, 10 * BASE_FEE, GAS_LIMIT, 2, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![&tx1, &tx2, &tx3, &tx4, &tx5, &tx6, &tx7, &tx8, &tx9]
                .into_iter()
                .map(|tx| (tx, true))
                .collect_vec(),
            expected_pool_size_change: 9,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 1024 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx7, &tx8, &tx9, &tx4, &tx2, &tx5, &tx3, &tx6],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn another_non_trivial_example() {
    let tx1 = make_legacy_tx(S1, 10 * BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_legacy_tx(S1, 5 * BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_legacy_tx(S2, 5 * BASE_FEE, GAS_LIMIT, 0, 10);
    let tx4 = make_legacy_tx(S2, 3 * BASE_FEE, GAS_LIMIT, 1, 10);
    let tx5 = make_legacy_tx(S3, 8 * BASE_FEE, GAS_LIMIT, 0, 10);
    let tx6 = make_legacy_tx(S3, 9 * BASE_FEE, GAS_LIMIT, 1, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![&tx1, &tx2, &tx3, &tx4, &tx5, &tx6]
                .into_iter()
                .map(|tx| (tx, true))
                .collect_vec(),
            expected_pool_size_change: 6,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 1024 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx5, &tx6, &tx3, &tx2, &tx4],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn attacker_tries_to_include_transaction_with_large_gas_limit_to_exit_proposal_creation_early() {
    let tx1 = make_legacy_tx(S1, 10 * BASE_FEE, 2 * PROPOSAL_GAS_LIMIT, 0, 10);
    let tx2 = make_legacy_tx(S2, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx3 = make_legacy_tx(S2, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx4 = make_legacy_tx(S2, BASE_FEE, GAS_LIMIT, 2, 10);
    let tx5 = make_legacy_tx(S2, BASE_FEE, GAS_LIMIT, 3, 10);
    let tx6 = make_legacy_tx(S2, BASE_FEE, GAS_LIMIT, 4, 10);
    let tx7 = make_legacy_tx(S2, BASE_FEE, GAS_LIMIT, 5, 10);
    let tx8 = make_legacy_tx(S2, BASE_FEE, GAS_LIMIT, 6, 10);
    let tx9 = make_legacy_tx(S2, BASE_FEE, GAS_LIMIT, 7, 10);
    let tx10 = make_legacy_tx(S2, BASE_FEE, GAS_LIMIT, 8, 10);
    let tx11 = make_legacy_tx(S2, BASE_FEE, GAS_LIMIT, 9, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![
                &tx1, &tx2, &tx3, &tx4, &tx5, &tx6, &tx7, &tx8, &tx9, &tx10, &tx11,
            ]
            .into_iter()
            .enumerate()
            .map(|(index, tx)| (tx, index != 0)) // tx1 does not get included
            .collect_vec(),
            expected_pool_size_change: 10,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx2, &tx3, &tx4, &tx5, &tx6, &tx7, &tx8, &tx9, &tx10, &tx11],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn suboptimal_block() {
    let tx1 = make_legacy_tx(S2, BASE_FEE, PROPOSAL_GAS_LIMIT / 10, 0, 10);
    let tx2 = make_legacy_tx(S2, BASE_FEE, PROPOSAL_GAS_LIMIT / 10, 1, 10);
    let tx3 = make_legacy_tx(S2, BASE_FEE, PROPOSAL_GAS_LIMIT / 10, 2, 10);
    let tx4 = make_legacy_tx(S2, BASE_FEE, PROPOSAL_GAS_LIMIT / 10, 3, 10);
    let tx5 = make_legacy_tx(S2, BASE_FEE, PROPOSAL_GAS_LIMIT / 10, 4, 10);
    let tx6 = make_legacy_tx(S2, BASE_FEE, PROPOSAL_GAS_LIMIT / 10, 5, 10);
    let tx7 = make_legacy_tx(S2, BASE_FEE, PROPOSAL_GAS_LIMIT / 10, 6, 10);
    let tx8 = make_legacy_tx(S2, BASE_FEE, PROPOSAL_GAS_LIMIT / 10, 7, 10);
    let tx9 = make_legacy_tx(S2, BASE_FEE, PROPOSAL_GAS_LIMIT / 10, 8, 10);
    let tx10 = make_legacy_tx(S2, BASE_FEE, PROPOSAL_GAS_LIMIT / 10, 9, 10);
    let tx11 = make_legacy_tx(S1, 2 * BASE_FEE, PROPOSAL_GAS_LIMIT, 0, 10);

    for reverse in [false, true] {
        let mut txs = vec![
            &tx1, &tx2, &tx3, &tx4, &tx5, &tx6, &tx7, &tx8, &tx9, &tx10, &tx11,
        ];

        if reverse {
            txs.reverse();
        }

        run_simple([
            TxPoolTestEvent::InsertTxs {
                txs: txs.into_iter().map(|tx| (tx, true)).collect_vec(),
                expected_pool_size_change: 11,
            },
            TxPoolTestEvent::CreateProposal {
                tx_limit: 11,
                gas_limit: PROPOSAL_GAS_LIMIT,
                expected_txs: vec![&tx11],
                add_to_blocktree: true,
            },
        ]);
    }
}

#[test]
#[traced_test]
fn zero_gas_limit() {
    let tx1 = make_legacy_tx(S1, BASE_FEE, 0, 0, 10);

    run_simple([TxPoolTestEvent::InsertTxs {
        txs: vec![(&tx1, false)],
        expected_pool_size_change: 0,
    }]);
}

#[test]
#[traced_test]
fn insertion_order() {
    let tx1 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_legacy_tx(S2, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx4 = make_legacy_tx(S2, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx5 = make_legacy_tx(S3, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx6 = make_legacy_tx(S3, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx7 = make_legacy_tx(S4, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx8 = make_legacy_tx(S4, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx9 = make_legacy_tx(S5, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx10 = make_legacy_tx(S5, BASE_FEE, GAS_LIMIT, 1, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![&tx1, &tx2, &tx3, &tx4, &tx5, &tx7, &tx8, &tx6, &tx9, &tx10]
                .into_iter()
                .map(|tx| (tx, true))
                .collect_vec(),
            expected_pool_size_change: 10,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 10,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx3, &tx5, &tx7, &tx9, &tx2, &tx4, &tx6, &tx8, &tx10],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_zero_nonce_included_in_block() {
    // The first transaction from an account with 0 nonce should be including in the block

    let tx1 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_nonce_gap() {
    // A transaction with nonce 1 should not be included in the block if a tx with nonce 0 is missing

    let tx1 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_intermediary_nonce_gap() {
    // A transaction with nonce 3 should not be included in the block if a tx with nonce 2 is missing

    let tx1 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 3, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true), (&tx2, true), (&tx3, true)],
            expected_pool_size_change: 3,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx2],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_nonce_exists_in_committed_block() {
    // A transaction with nonce 0 should not be included in the block if the latest nonce of the account is 0

    let tx1 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);

    let nonces = [(tx1.recover_signer().expect("signer is recoverable"), 1)]
        .into_iter()
        .collect();

    run_custom(
        make_test_block_policy,
        Some(nonces),
        [
            TxPoolTestEvent::InsertTxs {
                txs: vec![(&tx1, true), (&tx2, true)],
                expected_pool_size_change: 1,
            },
            TxPoolTestEvent::CreateProposal {
                tx_limit: 128,
                gas_limit: 10 * GAS_LIMIT,
                expected_txs: vec![&tx2],
                add_to_blocktree: true,
            },
        ],
    );
}

#[test]
#[traced_test]
fn test_unknown_account() {
    let tx1 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);

    run_custom(
        make_test_block_policy,
        Some(BTreeMap::default()),
        [
            TxPoolTestEvent::InsertTxs {
                txs: vec![(&tx1, false)],
                expected_pool_size_change: 0,
            },
            TxPoolTestEvent::Block(Arc::new(|pool| {
                assert!(pool.is_empty());
            })),
            TxPoolTestEvent::CreateProposal {
                tx_limit: 1,
                gas_limit: GAS_LIMIT,
                expected_txs: vec![],
                add_to_blocktree: true,
            },
        ],
    );
}

#[test]
#[traced_test]
fn test_nonce_exists_in_pending_block() {
    // A transaction with nonce 0 should not be included in the block if the latest nonce of the account is 0

    // generate two transactions, both with nonce = 0
    let tx1 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_legacy_tx(S1, BASE_FEE + 1, GAS_LIMIT, 0, 1000);

    let tx3 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 1,
            gas_limit: GAS_LIMIT,
            expected_txs: vec![&tx1],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx2, true), (&tx3, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx3],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_combine_nonces_of_blocks() {
    // TxPool should combine the nonces of commited block and pending blocks to check nonce

    let tx1 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 2, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::CommitPendingBlocks {
            num_blocks: 1,
            expected_committed_seq_num: 1,
        },
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx2, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx2],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx3, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx3],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_nonce_gap_maintained_across_proposals() {
    let tx1 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 2, 10);
    let tx4 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 3, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true), (&tx2, true), (&tx4, true)],
            expected_pool_size_change: 3,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx2],
            add_to_blocktree: false,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx2],
            add_to_blocktree: false,
        },
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx3, true)],
            expected_pool_size_change: 1,
        },
        // Even though proposals have been created, the txpool should still contain tx4!
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx2, &tx3, &tx4],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_nonce_gap_maintained_across_commit() {
    let tx1 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 2, 10);
    let tx4 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 3, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true), (&tx2, true), (&tx4, true)],
            expected_pool_size_change: 3,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx2],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::CommitPendingBlocks {
            num_blocks: 1,
            expected_committed_seq_num: 1,
        },
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx3, true)],
            expected_pool_size_change: 1,
        },
        // Even though block has been committed, the txpool should still contain tx4!
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx3, &tx4],
            add_to_blocktree: false,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx3, &tx4],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[should_panic]
fn test_tx_invalid_chain_id() {
    let tx1 = {
        let transaction = alloy_consensus::TxLegacy {
            chain_id: Some(999),
            nonce: 0,
            gas_price: BASE_FEE,
            gas_limit: GAS_LIMIT,
            to: alloy_primitives::TxKind::Call(alloy_primitives::Address::repeat_byte(0u8)),
            value: Default::default(),
            input: vec![0; 10].into(),
        };

        let signer = S1
            .to_string()
            .parse::<alloy_signer_local::PrivateKeySigner>()
            .unwrap();

        use alloy_consensus::SignableTransaction;
        use alloy_signer::SignerSync;

        let signature = signer
            .sign_hash_sync(&transaction.signature_hash())
            .unwrap();

        transaction.into_signed(signature).into()
    };

    run_custom(
        || EthBlockPolicy::new(GENESIS_SEQ_NUM, 0, 1, 1_000_000_000_000_000_000),
        None,
        [TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true)],
            expected_pool_size_change: 0,
        }],
    );
}

#[test]
fn test_same_account_priority_fee_ordering() {
    let tx_higher = make_eip1559_tx(S1, (BASE_FEE_PER_GAS + 50).into(), 20, GAS_LIMIT, 0, 10);
    let tx_lower = make_eip1559_tx(S1, (BASE_FEE_PER_GAS + 100).into(), 10, GAS_LIMIT, 0, 10);

    for (tx1, tx2) in [(&tx_higher, &tx_lower), (&tx_lower, &tx_higher)] {
        run_simple([
            TxPoolTestEvent::InsertTxs {
                txs: vec![(tx1, true), (tx2, tx2 == &tx_higher)],
                expected_pool_size_change: 1,
            },
            TxPoolTestEvent::CreateProposal {
                tx_limit: 2,
                gas_limit: GAS_LIMIT * 2,
                expected_txs: vec![&tx_higher],
                add_to_blocktree: false,
            },
        ]);
    }
}

#[test]
fn test_different_account_priority_fee_ordering() {
    for (addr1, addr2) in [(S1, S2), (S2, S1)] {
        let tx_higher =
            make_eip1559_tx(addr1, (BASE_FEE_PER_GAS + 50).into(), 20, GAS_LIMIT, 0, 10);
        let tx_lower =
            make_eip1559_tx(addr2, (BASE_FEE_PER_GAS + 100).into(), 10, GAS_LIMIT, 0, 10);

        for (tx1, tx2) in [(&tx_higher, &tx_lower), (&tx_lower, &tx_higher)] {
            run_simple([
                TxPoolTestEvent::InsertTxs {
                    txs: vec![(tx1, true), (tx2, true)],
                    expected_pool_size_change: 2,
                },
                TxPoolTestEvent::CreateProposal {
                    tx_limit: 2,
                    gas_limit: GAS_LIMIT * 2,
                    expected_txs: vec![&tx_higher, &tx_lower],
                    add_to_blocktree: false,
                },
            ]);
        }
    }
}

#[test]
fn test_missing_chain_id() {
    let tx: TxEnvelope = {
        let tx = TxLegacy {
            chain_id: None,
            nonce: 0,
            gas_price: BASE_FEE,
            gas_limit: GAS_LIMIT,
            to: TxKind::Call(Address::repeat_byte(0u8)),
            value: Default::default(),
            input: vec![0; 0].into(),
        };

        let signer = PrivateKeySigner::from_bytes(&S1).unwrap();
        let signature = signer.sign_hash_sync(&tx.signature_hash()).unwrap();

        tx.into_signed(signature).into()
    };

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 1,
            gas_limit: GAS_LIMIT,
            expected_txs: vec![&tx],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_large_batch_many_senders() {
    let txs: Vec<_> = (0..1024)
        .into_par_iter()
        .map(|idx| {
            let sender = B256::new(U256::from(idx as u64 + 1).to_be_bytes());
            make_legacy_tx(sender, BASE_FEE, GAS_LIMIT, 0, 10)
        })
        .collect();

    run_simple([
        TxPoolTestEvent::InsertTxBatch {
            txs: txs.iter().collect(),
            should_insert: true,
        },
        TxPoolTestEvent::Block(Arc::new({
            let txs = txs.clone();

            move |pool| {
                let EthTxPoolSnapshot { pending, tracked } = pool.generate_snapshot();

                let mut txs = txs.clone();
                txs.retain(|tx| !pending.contains(tx.tx_hash()) && !tracked.contains(tx.tx_hash()));

                assert!(txs.is_empty())
            }
        })),
    ]);
}

#[test]
#[traced_test]
fn test_exceed_byte_limit() {
    let tx1 = make_legacy_tx(
        S1,
        BASE_FEE,
        100_000_000,
        0,
        PROPOSAL_SIZE_LIMIT as usize - 111,
    );
    assert_eq!(tx1.length() as u64, PROPOSAL_SIZE_LIMIT);

    let tx2 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);

    run_simple([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true), (&tx2, true)],
            expected_pool_size_change: 2,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 2,
            gas_limit: PROPOSAL_GAS_LIMIT,
            expected_txs: vec![&tx1],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 2,
            gas_limit: PROPOSAL_GAS_LIMIT,
            expected_txs: vec![&tx2],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
fn test_compute_balances() {
    let tx1 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_legacy_tx(S1, BASE_FEE, GAS_LIMIT, 0, 1000);
    let txns = vec![tx1, tx2];

    let nonces: BTreeMap<Address, u64> = BTreeMap::new();
    let state_backend =
        { InMemoryStateInner::new(Balance::MAX, SeqNum(4), InMemoryBlockState::genesis(nonces)) };
    let mut current_round = 1u64;
    let mut current_seq_num = 1u64;

    let extending_blocks = None;
    let block_policy = make_test_block_policy();
    let block = generate_block_with_txs(
        Round(current_round),
        SeqNum(current_seq_num),
        txns.into_iter()
            .map(|tx| {
                let signer = tx.recover_signer().unwrap();
                Recovered::new_unchecked(tx, signer)
            })
            .collect(),
    );

    let tx_signers = block
        .validated_txns
        .iter()
        .map(|txn| txn.signer())
        .collect_vec();

    let balances = block_policy.compute_account_base_balances(
        block.get_seq_num(),
        &state_backend,
        extending_blocks,
        tx_signers.iter(),
    );
}
