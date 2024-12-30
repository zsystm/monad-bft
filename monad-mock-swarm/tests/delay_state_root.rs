#[cfg(test)]
mod test {
    use std::{
        collections::{BTreeMap, BTreeSet, HashSet},
        time::Duration,
    };

    use alloy_rlp::Decodable;
    use itertools::Itertools;
    use monad_async_state_verify::{majority_threshold, PeerAsyncStateVerify};
    use monad_consensus_types::block::BlockType;
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
        NopPubKey, NopSignature,
    };
    use monad_eth_block_policy::EthBlockPolicy;
    use monad_eth_block_validator::EthValidator;
    use monad_eth_ledger::MockEthLedger;
    use monad_eth_testutil::{make_tx, secret_to_eth_address};
    use monad_eth_tx::EthSignedTransaction;
    use monad_eth_txpool::EthTxPool;
    use monad_eth_types::{Balance, EthAddress};
    use monad_mock_swarm::{
        mock::TimestamperConfig,
        mock_swarm::{Nodes, SwarmBuilder},
        node::NodeBuilder,
        swarm_relation::SwarmRelation,
        terminator::UntilTerminator,
        verifier::{happy_path_tick_by_block, MockSwarmVerifier},
    };
    use monad_multi_sig::MultiSig;
    use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
    use monad_state::{MonadMessage, VerifiedMonadMessage};
    use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
    use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
    use monad_transformer::{
        DropTransformer, GenericTransformer, GenericTransformerPipeline, LatencyTransformer,
        PartitionTransformer, ID,
    };
    use monad_types::{NodeId, Round, SeqNum, GENESIS_SEQ_NUM};
    use monad_updaters::{
        ledger::MockableLedger, state_root_hash::MockStateRootHashScheduler,
        statesync::MockStateSyncExecutor,
    };
    use monad_validator::{
        simple_round_robin::SimpleRoundRobin,
        validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory},
    };
    use tracing_subscriber::{
        fmt::{format::FmtSpan, Layer as FmtLayer},
        layer::SubscriberExt,
        reload::Handle,
        EnvFilter, Registry,
    };
    use tracing::{debug, warn, info};
    use reth_primitives::B256;

    pub struct EthSwarm;
    impl SwarmRelation for EthSwarm {
        type SignatureType = NopSignature;
        type SignatureCollectionType = MultiSig<Self::SignatureType>;
        type StateBackendType = InMemoryState;
        type BlockPolicyType = EthBlockPolicy<Self::SignatureCollectionType>;

        type TransportMessage =
            VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;

        type BlockValidator = EthValidator;
        type ValidatorSetTypeFactory =
            ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
        type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
        type TxPool = EthTxPool;
        type Ledger = MockEthLedger<Self::SignatureType, Self::SignatureCollectionType>;
        type AsyncStateRootVerify = PeerAsyncStateVerify<
            Self::SignatureCollectionType,
            <Self::ValidatorSetTypeFactory as ValidatorSetTypeFactory>::ValidatorSetType,
        >;

        type RouterScheduler = NoSerRouterScheduler<
            CertificateSignaturePubKey<Self::SignatureType>,
            MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
            VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        >;

        type Pipeline = GenericTransformerPipeline<
            CertificateSignaturePubKey<Self::SignatureType>,
            Self::TransportMessage,
        >;

        type StateRootHashExecutor =
            MockStateRootHashScheduler<Self::SignatureType, Self::SignatureCollectionType>;
        type StateSyncExecutor =
            MockStateSyncExecutor<Self::SignatureType, Self::SignatureCollectionType>;
    }

    const CONSENSUS_DELTA: Duration = Duration::from_millis(100);
    const BASE_FEE: u128 = 1000;
    const GAS_LIMIT: u64 = 30000;

    fn generate_eth_swarm(
        num_nodes: u16,
        existing_accounts: impl IntoIterator<Item = EthAddress>,
    ) -> Nodes<EthSwarm> {
        let execution_delay = SeqNum(4);

        let existing_nonces: BTreeMap<_, _> =
            existing_accounts.into_iter().map(|acc| (acc, 0)).collect();

        let state_configs = make_state_configs::<EthSwarm>(
            num_nodes,
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            || EthTxPool::new(true),
            || EthValidator::new(10_000, 1_000_000, 1337),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || {
                InMemoryStateInner::new(
                    Balance::MAX,
                    execution_delay,
                    InMemoryBlockState::genesis(existing_nonces.clone()),
                )
            },
            PeerAsyncStateVerify::new,
            execution_delay,          // state_root_delay
            CONSENSUS_DELTA,          // delta
            Duration::from_millis(0), // vote pace
            10,                       // proposal_tx_limit
            SeqNum(2000),             // val_set_update_interval
            Round(50),                // epoch_start_delay
            majority_threshold,       // state root quorum threshold
            SeqNum(100),              // state_sync_threshold
        );
        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();
        let swarm_config = SwarmBuilder::<EthSwarm>(
            state_configs
                .into_iter()
                .enumerate()
                .map(|(seed, state_builder)| {
                    let validators = state_builder.forkpoint.validator_sets[0].clone();
                    let state_backend = state_builder.state_backend.clone();
                    NodeBuilder::<EthSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockStateRootHashScheduler::new(validators.validators.clone(), SeqNum(2000)),
                        MockEthLedger::new(state_backend.clone()),
                        MockStateSyncExecutor::new(
                            state_backend,
                            validators
                                .validators
                                .0
                                .into_iter()
                                .map(|v| v.node_id)
                                .collect(),
                        ),
                        vec![GenericTransformer::Latency(LatencyTransformer::new(
                            CONSENSUS_DELTA,
                        ))],
                        vec![],
                        TimestamperConfig::default(),
                        seed.try_into().unwrap(),
                    )
                })
                .collect(),
        );

        swarm_config.build()
    }

    fn verify_transactions_in_ledger(
        swarm: &Nodes<EthSwarm>,
        node_ids: Vec<ID<NopPubKey>>,
        txns: Vec<EthSignedTransaction>,
    ) -> bool {
        let txns: HashSet<_> = HashSet::from_iter(txns.iter().map(|t| t.hash()));
        for node_id in node_ids {
            let state = swarm.states().get(&node_id).unwrap();
            let mut txns_to_see = txns.clone();
            for (round, block) in state.executor.ledger().get_finalized_blocks() {
                let decoded_txns =
                    Vec::<EthSignedTransaction>::decode(&mut block.payload.txns.as_ref()).unwrap();

                for txn in decoded_txns {
                    let txn_hash = txn.hash();
                    if txns_to_see.contains(&txn_hash) {
                        txns_to_see.remove(&txn_hash);
                    } else {
                        info!(
                            "Unexpected transaction in block round {}. SeqNum: {}, NodeID: {}, TxnHash: {}, Nonce: {}",
                            round.0, block.get_seq_num().0, node_id, txn_hash, txn.nonce()
                        );
                        return false;
                    }
                }
            }

            if !txns_to_see.is_empty() {
                info!(
                    "Expected transactions don't exist. NodeID: {}, TxnHashes: {:?}",
                    node_id, txns_to_see
                );
                return false;
            }
        }

        true
    }

    #[test]
    fn delay_state_root_block() {
        let sender_1_key = B256::random();
        let mut swarm = generate_eth_swarm(2, vec![secret_to_eth_address(sender_1_key)]);

        let node_ids = swarm.states().keys().copied().collect_vec();
        let node_1_id = node_ids[0];

        // step until nodes are ready to receive txs (post statesync)
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(1))
            .is_some()
        {}

        let mut expected_txns = Vec::new();
        // Send 10 transactions with nonces 0..10 to Node 1. Leader for round 1
        for nonce in 0..10 {
            let eth_txn = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn.envelope_encoded().into());

            expected_txns.push(eth_txn);
        }

        // Can not produce blocks until state root is available, state root is available at block 5 = 1 + execution_delay.
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(4))
            .is_some()
        {}

        // Transactions are not in the ledger
        assert!(!verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns.clone()
        ));

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(5))
            .is_some()
        {}

        // All transactions should be in the ledger
        assert!(verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns.clone()
        ));
    }

    #[test]
    fn delay_state_root_timer() {
        let sender_1_key = B256::random();
        let mut swarm = generate_eth_swarm(2, vec![secret_to_eth_address(sender_1_key)]);

        let node_ids = swarm.states().keys().copied().collect_vec();
        let node_1_id = node_ids[0];

        // step until nodes are ready to receive txs (post statesync)
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(1))
            .is_some()
        {}

        let mut expected_txns = Vec::new();
        // Send 10 transactions with nonces 0..10 to Node 1. Leader for round 1
        for nonce in 0..10 {
            let eth_txn = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn.envelope_encoded().into());

            expected_txns.push(eth_txn);
        }

        // Cant produce blocks until state root is available
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(5))
            .is_some()
        {}

        // The first 10 transactions should be in the ledger
        assert!(verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns.clone()
        ));

        swarm.set_state_root_poll(node_1_id, false);

        // Move to the last block that can validate the state root with execution results.
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(9))
            .is_some()
        {}

        // Send 10 different transactions
        for nonce in 10..20 {
            let eth_txn = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 1000);

            swarm.send_transaction(node_1_id, eth_txn.envelope_encoded().into());

            expected_txns.push(eth_txn);
        }

        while swarm
            .step_until(&mut UntilTerminator::new().until_tick(Duration::from_secs(10)))
            .is_some()
        {}

        // New transactions are not in the ledger.
        assert!(!verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns.clone()
        ));

        swarm.set_state_root_poll(node_1_id, true);

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(13))
            .is_some()
        {}

        // All transactions should be in the ledger
        assert!(verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns.clone()
        ));
    }
}
