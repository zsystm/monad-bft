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

use std::{collections::BTreeSet, time::Duration};

use itertools::Itertools;
use monad_chain_config::{revision::ChainParams, MockChainConfig};
use monad_consensus_types::{block::PassthruBlockPolicy, block_validator::MockValidator};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_mock_swarm::{
    mock::TimestamperConfig,
    mock_swarm::SwarmBuilder,
    node::NodeBuilder,
    swarm_relation::NoSerSwarm,
    terminator::UntilTerminator,
    verifier::{happy_path_tick_by_block, MockSwarmVerifier},
};
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_state_backend::InMemoryStateInner;
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_transformer::{GenericTransformer, LatencyTransformer, ID};
use monad_types::{Balance, NodeId, Round, SeqNum};
use monad_updaters::{
    ledger::MockLedger, state_root_hash::MockStateRootHashNop, statesync::MockStateSyncExecutor,
    txpool::MockTxPoolExecutor,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};

static CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    max_reserve_balance: 1_000_000_000_000_000_000,
    vote_pace: Duration::from_millis(0),
};

/// Timestamp adjustment is disabled in consensus state, but timestamp is still
/// checked when voting. This test is expected to fail until we fix and
/// re-enable timestamp adjustment
#[ignore]
#[test]
fn drift_one_node() {
    let delta = Duration::from_millis(30);
    let latency = Duration::from_millis(20);
    let state_configs = make_state_configs::<NoSerSwarm>(
        4, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(Balance::MAX, SeqNum(4)),
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
    let swarm_config = SwarmBuilder::<NoSerSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, state_builder)| {
                let state_backend = state_builder.state_backend.clone();
                let validators = state_builder.locked_epoch_validators[0].clone();
                let seed: u64 = seed.try_into().unwrap();
                let drift = if seed == 1 {
                    Duration::from_millis(1)
                } else {
                    Duration::from_millis(0)
                };
                let ts_config = TimestamperConfig {
                    timestamp_drift: drift,
                    ..Default::default()
                };
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
                    vec![GenericTransformer::Latency(LatencyTransformer::new(
                        latency,
                    ))],
                    vec![],
                    ts_config,
                    seed,
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    while swarm
        .step_until(&mut UntilTerminator::new().until_block(1026))
        .is_some()
    {}

    let node_ids = swarm.states().keys().copied().collect_vec();
    // the calculation is correct with two nodes because NoSerRouterScheduler
    // always sends the message over the network/transformer, even for it's for
    // self. Otherwise the time is shorter with two nodes, like QUIC test
    let mut verifier =
        MockSwarmVerifier::default().tick_range(happy_path_tick_by_block(1026, latency), latency);
    verifier.metrics_happy_path(&node_ids, &swarm);
    assert!(verifier.verify(&swarm));

    swarm_ledger_verification(&swarm, 1024);
}
