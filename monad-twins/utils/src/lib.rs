pub mod twin_reader;

use std::{collections::BTreeMap, time::Duration};

use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_metrics::MetricsPolicy;
use monad_mock_swarm::{
    mock::TimestamperConfig,
    mock_swarm::SwarmBuilder,
    node::NodeBuilder,
    swarm_relation::SwarmRelation,
    transformer::{MonadMessageTransformer, MonadMessageTransformerPipeline, TwinsTransformer},
};
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_state_backend::InMemoryState;
use monad_transformer::RandLatencyTransformer;
use monad_types::{ExecutionProtocol, MockableFinalizedHeader, NodeId, SeqNum};
use monad_updaters::{
    ledger::MockLedger, state_root_hash::MockStateRootHashNop, statesync::MockStateSyncExecutor,
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use twin_reader::TWINS_STATE_ROOT_DELAY;

use crate::twin_reader::{TwinsNodeConfig, TwinsTestCase};

pub fn run_twins_test<ST, SCT, EPT, MP, S>(seed: u64, test_case: TwinsTestCase<S>)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
    MP: MetricsPolicy,
    S: SwarmRelation<
        SignatureType = ST,
        SignatureCollectionType = SCT,
        StateBackendType = InMemoryState,
        Pipeline = MonadMessageTransformerPipeline<CertificateSignaturePubKey<ST>>,
        RouterScheduler = NoSerRouterScheduler<
            CertificateSignaturePubKey<ST>,
            MonadMessage<ST, SCT, EPT>,
            VerifiedMonadMessage<ST, SCT, EPT>,
        >,
        StateRootHashExecutor = MockStateRootHashNop<ST, SCT, EPT, MP>,
        StateSyncExecutor = MockStateSyncExecutor<ST, SCT, EPT>,
        Ledger = MockLedger<ST, SCT, EPT>,
        MetricsPolicy = MP,
    >,
    S::TxPoolExecutor: Default,
{
    let TwinsTestCase {
        description: _,
        allow_block_sync,
        liveness,
        mut terminator,
        delta,
        duplicates,
        nodes,
    } = test_case;

    let ids = nodes.keys().copied().collect::<Vec<_>>();
    let mut rng = ChaChaRng::seed_from_u64(seed);
    let mut swarm = SwarmBuilder::<S>(vec![]).build().can_have_duplicate_peer();

    for TwinsNodeConfig {
        id,
        state_config: state_builder,
        default_partition,
        partition,
    } in nodes.into_values()
    {
        let twins_transformer = TwinsTransformer::new(
            duplicates.clone(),
            partition,
            default_partition,
            !allow_block_sync,
        );

        let outbound_pipeline = vec![
            MonadMessageTransformer::Twins(twins_transformer),
            MonadMessageTransformer::RandLatency(RandLatencyTransformer::new(
                rng.gen(),
                Duration::from_millis(delta),
            )),
        ];
        let state_backend = state_builder.state_backend.clone();
        let validators = state_builder.locked_epoch_validators[0].clone();
        swarm.add_state(NodeBuilder::<S>::new(
            id,
            state_builder,
            NoSerRouterConfig::new(
                ids.iter()
                    .map(|id| NodeId::new(id.get_peer_id().pubkey()))
                    .collect(),
            )
            .build(),
            MockStateRootHashNop::new(
                validators.validators.clone(),
                SeqNum(TWINS_STATE_ROOT_DELAY), // ?? val_set_interval?
            ),
            S::TxPoolExecutor::default(),
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
            outbound_pipeline,
            vec![],
            TimestamperConfig::default(),
            seed,
        ));
    }

    while swarm.step_until(&mut terminator.clone()).is_some() {}

    if let Some(liveness_length) = liveness {
        let liveness_transformer =
            TwinsTransformer::new(duplicates, BTreeMap::new(), ids.clone(), false);
        let pipeline = vec![
            MonadMessageTransformer::Twins(liveness_transformer),
            MonadMessageTransformer::RandLatency(RandLatencyTransformer::new(
                rng.gen(),
                Duration::from_millis(delta),
            )),
        ];

        for id in ids {
            swarm.update_outbound_pipeline(&id, pipeline.clone());
        }
        // extend the expected termination condition
        terminator.extend_all(liveness_length);
        // run all nodes all connected until expected_block + liveness block check is achieved
        while swarm.step_until(&mut terminator.clone()).is_some() {}
    }
}
