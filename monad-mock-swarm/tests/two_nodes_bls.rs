use std::{collections::BTreeSet, time::Duration};

use itertools::Itertools;
use monad_bls::BlsSignatureCollection;
use monad_chain_config::{
    revision::{ChainParams, MockChainRevision},
    MockChainConfig,
};
use monad_consensus_types::{
    block::{MockExecutionProtocol, PassthruBlockPolicy},
    block_validator::MockValidator,
    metrics::StateMetrics,
};
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_types::Balance;
use monad_metrics::MockMetricsPolicy;
use monad_mock_swarm::{
    mock::TimestamperConfig,
    mock_swarm::SwarmBuilder,
    node::NodeBuilder,
    swarm_relation::SwarmRelation,
    terminator::UntilTerminator,
    verifier::{happy_path_tick_by_block, MockSwarmVerifier},
};
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
use monad_secp::SecpSignature;
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_state_backend::{InMemoryState, InMemoryStateInner};
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_transformer::{GenericTransformer, GenericTransformerPipeline, LatencyTransformer, ID};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::{
    ledger::MockLedger, state_root_hash::MockStateRootHashNop, statesync::MockStateSyncExecutor,
    txpool::MockTxPoolExecutor,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
struct BLSSwarm;
impl SwarmRelation for BLSSwarm {
    type SignatureType = SecpSignature;
    type SignatureCollectionType =
        BlsSignatureCollection<CertificateSignaturePubKey<Self::SignatureType>>;
    type ExecutionProtocolType = MockExecutionProtocol;
    type StateBackendType = InMemoryState;
    type BlockPolicyType = PassthruBlockPolicy;
    type ChainConfigType = MockChainConfig;
    type ChainRevisionType = MockChainRevision;

    type TransportMessage = VerifiedMonadMessage<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;

    type BlockValidator = MockValidator;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
    type Ledger =
        MockLedger<Self::SignatureType, Self::SignatureCollectionType, Self::ExecutionProtocolType>;

    type RouterScheduler = NoSerRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >,
        VerifiedMonadMessage<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >,
    >;

    type Pipeline = GenericTransformerPipeline<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::TransportMessage,
    >;

    type StateRootHashExecutor = MockStateRootHashNop<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
        Self::MetricsPolicy,
    >;
    type TxPoolExecutor = MockTxPoolExecutor<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
        Self::BlockPolicyType,
        Self::StateBackendType,
        Self::MetricsPolicy,
    >;
    type StateSyncExecutor = MockStateSyncExecutor<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;
    type MetricsPolicy = MockMetricsPolicy;
}

static CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    vote_pace: Duration::from_millis(5),
};

#[test]
fn two_nodes_bls() {
    tracing_subscriber::fmt::init();

    let delta = Duration::from_millis(20);

    let state_configs = make_state_configs::<BLSSwarm>(
        2, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(Balance::MAX, SeqNum(4)),
        StateMetrics::default,
        SeqNum(4),                           // execution_delay
        delta,                               // delta
        MockChainConfig::new(&CHAIN_PARAMS), // chain config
        SeqNum(2000),                        // val_set_update_interval
        Round(50),                           // epoch_start_delay
        SeqNum(100),                         // state_sync_threshold
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();
    let swarm_config = SwarmBuilder::<BLSSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, state_builder)| {
                let state_backend = state_builder.state_backend.clone();
                let validators = state_builder.locked_epoch_validators[0].clone();
                NodeBuilder::<BLSSwarm>::new(
                    ID::new(NodeId::new(state_builder.key.pubkey())),
                    state_builder,
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockStateRootHashNop::new(validators.validators.clone(), SeqNum(2000)),
                    MockTxPoolExecutor::default(),
                    MockLedger::new(state_backend.clone()),
                    MockStateSyncExecutor::new(
                        state_backend,
                        validators
                            .validators
                            .0
                            .into_iter()
                            .map(|v| v.node_id)
                            .collect(),
                    ),
                    vec![GenericTransformer::Latency(LatencyTransformer::new(delta))],
                    vec![],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    while swarm
        .step_until(&mut UntilTerminator::new().until_block(100))
        .is_some()
    {}
    swarm_ledger_verification(&swarm, 98);

    // the calculation is correct with two nodes because NoSerRouterScheduler
    // always sends the message over the network/transformer, even for it's for
    // self
    let mut verifier =
        MockSwarmVerifier::default().tick_range(happy_path_tick_by_block(100, delta), delta);

    let node_ids = swarm.states().keys().copied().collect_vec();
    verifier.metrics_happy_path(&node_ids, &swarm);

    assert!(verifier.verify(&swarm));
}
