use std::{
    collections::{BTreeSet, HashSet},
    env,
    time::{Duration, Instant},
};

use itertools::Itertools;
use monad_chain_config::{revision::ChainParams, MockChainConfig};
use monad_consensus_types::{
    block::PassthruBlockPolicy, block_validator::MockValidator, metrics::StateMetrics,
};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_eth_types::Balance;
use monad_metrics::{Counter, NoopMetricsPolicy};
use monad_mock_swarm::{
    fetch_metric_counter, mock::TimestamperConfig, mock_swarm::SwarmBuilder, node::NodeBuilder,
    swarm_relation::NoSerSwarm, terminator::UntilTerminator, verifier::MockSwarmVerifier,
};
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_state_backend::InMemoryStateInner;
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_transformer::{
    GenericTransformer, LatencyTransformer, PartitionTransformer, ReplayTransformer,
    TransformerReplayOrder, ID,
};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::{
    ledger::{MockLedger, MockableLedger},
    state_root_hash::MockStateRootHashNop,
    statesync::MockStateSyncExecutor,
    txpool::MockTxPoolExecutor,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
use rand::{rngs::StdRng, Rng, SeedableRng};
use test_case::test_case;

static CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    vote_pace: Duration::from_millis(0),
};

#[test]
#[ignore = "cron_test"]
fn all_messages_delayed_cron() {
    let time_seconds = match env::var("ALL_MESSAGES_DELAYED_TIME_SECONDS") {
        Ok(v) => v.parse().unwrap(),
        Err(_e) => {
            println!("ALL_MESSAGES_DELAYED_TIME_SECONDS is not set, using default of 60");
            60
        }
    };

    let mut seed = match env::var("RANDOM_TEST_SEED") {
        Ok(v) => v.parse().unwrap(),
        Err(_e) => {
            println!("RANDOM_TEST_SEED is not set, using default seed 0");
            0
        }
    };

    let start_time = Instant::now();

    let mut generator = StdRng::seed_from_u64(seed);
    while start_time.elapsed() < Duration::from_secs(time_seconds) {
        seed = generator.gen();
        match all_messages_delayed(TransformerReplayOrder::Random(seed)) {
            Ok(_) => {}
            Err(_) => {
                panic!("failing seed is {}", seed);
            }
        }
    }
}

#[test_case(TransformerReplayOrder::Forward; "in order")]
#[test_case(TransformerReplayOrder::Reverse; "reverse order")]
#[test_case(TransformerReplayOrder::Random(1); "random seed 1")]
#[test_case(TransformerReplayOrder::Random(2); "random seed 2")]
#[test_case(TransformerReplayOrder::Random(3); "random seed 3")]
#[test_case(TransformerReplayOrder::Random(4); "random seed 4")]
#[test_case(TransformerReplayOrder::Random(5); "random seed 5")]
fn all_messages_delayed(direction: TransformerReplayOrder) -> Result<(), String> {
    // tracing_subscriber::fmt::init();
    let delta = Duration::from_millis(20);
    let max_blocksync_retries = 5;
    let state_configs = make_state_configs::<NoSerSwarm>(
        4, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(Balance::MAX, SeqNum(1)),
        StateMetrics::default,
        // due to the burst behavior of replay-transformer, its okay to
        // have delay as 1
        //
        // TODO-4?: Make Replay Transformer's stored message not burst
        // within the same Duration
        SeqNum(1),                           // execution_delay
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

    let first_node = ID::new(*all_peers.first().unwrap());

    let mut filter_peers = HashSet::new();
    filter_peers.insert(first_node);

    let swarm_config = SwarmBuilder::<NoSerSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, state_builder)| {
                let state_backend = state_builder.state_backend.clone();
                let validators = state_builder.locked_epoch_validators[0].clone();
                NodeBuilder::<NoSerSwarm>::new(
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
                    vec![
                        GenericTransformer::Partition(PartitionTransformer(filter_peers.clone())),
                        GenericTransformer::Replay(ReplayTransformer::new(
                            Duration::from_millis(500),
                            direction.clone(),
                        )),
                    ],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    // run the swarm to before the replay and record the longest ledger length
    while swarm
        .step_until(&mut UntilTerminator::new().until_tick(Duration::from_millis(499)))
        .is_some()
    {}

    let longest_ledger_before = swarm
        .states()
        .values()
        .map(|s| {
            MockableLedger::<NoopMetricsPolicy>::get_finalized_blocks(s.executor.ledger()).len()
        })
        .max()
        .unwrap();

    let nodes_except_first: Vec<ID<_>> = swarm
        .states()
        .keys()
        .filter(|&node_id| *node_id != first_node)
        .copied()
        .collect_vec();

    let mut verifier_before_delayed_messages = MockSwarmVerifier::default();
    verifier_before_delayed_messages
        .metric_exact(
            &nodes_except_first,
            // fetch_metric!(blocksync_events.self_headers_request),
            ("blocksync_events . self_headers_request", |s| {
                Counter::read(&s.blocksync_events.self_headers_request)
            }),
            0,
        )
        .metric_exact(
            &nodes_except_first,
            fetch_metric_counter!(blocksync_events.self_payload_request),
            0,
        )
        // handle proposal for all blocks in ledger
        .metric_minimum(
            &nodes_except_first,
            fetch_metric_counter!(consensus_events.handle_proposal),
            longest_ledger_before as u64,
        )
        // vote for all blocks in ledger
        .metric_minimum(
            &nodes_except_first,
            fetch_metric_counter!(consensus_events.created_vote),
            longest_ledger_before as u64,
        )
        // enter rounds using QC except round 2
        .metric_minimum(
            &nodes_except_first,
            fetch_metric_counter!(consensus_events.enter_new_round_qc),
            longest_ledger_before as u64 - 1,
        );

    assert!(verifier_before_delayed_messages.verify(&swarm));

    while swarm
        .step_until(&mut UntilTerminator::new().until_tick(Duration::from_millis(1000)))
        .is_some()
    {}

    // validators should produce the same number of blocks as a working swarm
    // after replay
    //
    // the reverse function of happy_path_tick_by_block
    let expected_blocks = (Duration::from_millis(500).as_millis() / delta.as_millis() - 1) / 2 - 3;
    // reason for -3:
    // -2 because there are 2 uncommitted blocks in the blocktree
    // -1 because the blackout node can't propose blocks when there are unvalidated
    //    blocks in its blocktree
    swarm_ledger_verification(&swarm, longest_ledger_before + expected_blocks as usize);

    let longest_ledger_after = swarm
        .states()
        .values()
        .map(|s| {
            MockableLedger::<NoopMetricsPolicy>::get_finalized_blocks(s.executor.ledger()).len()
        })
        .max()
        .unwrap();

    let blocksync_requests_range = match direction {
        // when replayed forward, node should populate blocktree in order
        TransformerReplayOrder::Forward => (0, 7),
        // when replayed in reverse, only one request is done from high_qc
        TransformerReplayOrder::Reverse => (longest_ledger_before + 1, longest_ledger_before + 2),
        // when replayed in random order, could be any number of requests
        // max_blocksync_retries is to ensure that failed blocksync is not
        // triggered too many times
        TransformerReplayOrder::Random(_) => (0, longest_ledger_before + 2 + max_blocksync_retries),
        // TODO add a TransformerReplayOrder::Forward, but with the first block skipped.
        // We expect to see 1 blocksync request happen in this case.
    };

    let mut verifier_after_delayed_messages = MockSwarmVerifier::default();
    verifier_after_delayed_messages
        .metric_exact(
            &nodes_except_first,
            fetch_metric_counter!(blocksync_events.self_headers_request),
            0,
        )
        .metric_exact(
            &nodes_except_first,
            fetch_metric_counter!(blocksync_events.self_payload_request),
            0,
        )
        // handle proposal for all blocks in ledger
        .metric_minimum(
            &nodes_except_first,
            fetch_metric_counter!(consensus_events.handle_proposal),
            longest_ledger_after as u64,
        )
        // vote for all blocks in ledger
        .metric_minimum(
            &nodes_except_first,
            fetch_metric_counter!(consensus_events.created_vote),
            longest_ledger_after as u64,
        )
        // enter rounds using QC except round 2
        .metric_minimum(
            &nodes_except_first,
            fetch_metric_counter!(consensus_events.enter_new_round_qc),
            longest_ledger_after as u64 - 1,
        )
        .metric_range(
            &vec![first_node],
            fetch_metric_counter!(blocksync_events.self_headers_request),
            blocksync_requests_range.0 as u64,
            blocksync_requests_range.1 as u64,
        );

    if !verifier_after_delayed_messages.verify(&swarm) {
        return Err("verification failed".to_string());
    }

    Ok(())
}
