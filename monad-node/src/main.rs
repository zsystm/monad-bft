use std::{
    collections::{BTreeSet, HashMap},
    marker::PhantomData,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    process,
    sync::Arc,
    time::Duration,
};

use agent::AgentBuilder;
use alloy_rlp::{Decodable, Encodable};
use chrono::Utc;
use clap::CommandFactory;
use futures_util::{FutureExt, StreamExt};
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    metrics::Metrics,
    quorum_certificate::HALT_TIP,
    signature_collection::SignatureCollection,
    validator_data::{ValidatorSetDataWithEpoch, ValidatorsConfig},
};
use monad_control_panel::{ipc::ControlPanelIpcReceiver, TracingReload};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
    },
    signing_domain,
};
use monad_dataplane::DataplaneBuilder;
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_block_validator::EthValidator;
use monad_eth_txpool_executor::{EthTxPoolExecutor, EthTxPoolIpcConfig};
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{LogFriendlyMonadEvent, Message, MonadEvent};
use monad_ledger::MonadBlockFileLedger;
use monad_node_config::{
    ExecutionProtocolType, FullNodeIdentityConfig, NodeBootstrapConfig, NodeConfig,
    PeerDiscoveryConfig, SignatureCollectionType, SignatureType,
};
use monad_peer_discovery::{
    discovery::{PeerDiscovery, PeerDiscoveryBuilder},
    MonadNameRecord, NameRecord,
};
use monad_pprof::start_pprof_server;
use monad_raptorcast::config::{
    GroupSchedulingConfig, RaptorCastConfig, RaptorCastConfigPrimary, RaptorCastConfigSecondary,
    RaptorCastConfigSecondaryClient, RaptorCastConfigSecondaryPublisher,
    SecondaryRaptorCastModeConfig,
};
use monad_router_multi::MultiRouter;
use monad_state::{MonadMessage, MonadStateBuilder, VerifiedMonadMessage};
use monad_state_backend::StateBackendThreadClient;
use monad_statesync::StateSync;
use monad_triedb_cache::StateBackendCache;
use monad_triedb_utils::TriedbReader;
use monad_types::{DropTimer, Epoch, NodeId, Round, SeqNum, GENESIS_SEQ_NUM};
use monad_updaters::{
    checkpoint::FileCheckpoint, config_loader::ConfigLoader, loopback::LoopbackExecutor,
    parent::ParentExecutor, timer::TokioTimer, tokio_timestamp::TokioTimestamp,
    triedb_state_root_hash::StateRootHashTriedbPoll, BoxUpdater, Updater,
};
use monad_validator::{
    validator_set::ValidatorSetFactory, weighted_round_robin::WeightedRoundRobin,
};
use monad_wal::{wal::WALoggerConfig, PersistenceLoggerBuilder};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use rand_chacha::{rand_core::SeedableRng, ChaCha8Rng};
use tokio::signal::unix::{signal, SignalKind};
use tracing::{debug, error, event, info, warn, Instrument, Level};
use tracing_manytrace::{ManytraceLayer, TracingExtension};
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer as FmtLayer},
    layer::SubscriberExt,
    Layer,
};

use self::{cli::Cli, error::NodeSetupError, state::NodeState};

mod cli;
mod error;
mod state;

#[cfg(all(not(target_env = "msvc"), feature = "jemallocator"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "jemallocator")]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

const CLIENT_VERSION: &str = env!("VERGEN_GIT_DESCRIBE");
const STATESYNC_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

const EXECUTION_DELAY: u64 = 3;

fn main() {
    let mut cmd = Cli::command();

    let node_state = NodeState::setup(&mut cmd).unwrap_or_else(|e| cmd.error(e.kind(), e).exit());

    rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .thread_name(|i| format!("monad-bft-rn-{}", i))
        .build_global()
        .map_err(Into::into)
        .unwrap_or_else(|e: NodeSetupError| cmd.error(e.kind(), e).exit());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(Into::into)
        .unwrap_or_else(|e: NodeSetupError| cmd.error(e.kind(), e).exit());

    let (reload_handle, _agent) = setup_tracing(&node_state)
        .unwrap_or_else(|e: NodeSetupError| cmd.error(e.kind(), e).exit());

    drop(cmd);

    if !node_state.pprof.is_empty() {
        runtime.spawn({
            let pprof = node_state.pprof.clone();
            async {
                let server = match start_pprof_server(pprof) {
                    Ok(server) => server,
                    Err(err) => {
                        error!("failed to start pprof server: {}", err);
                        return;
                    }
                };
                if let Err(err) = server.await {
                    error!("pprof server failed: {}", err);
                }
            }
        });
    }

    if let Err(e) = runtime.block_on(run(node_state, reload_handle)) {
        tracing::error!("monad consensus node crashed: {:?}", e);
    }
}

fn setup_tracing(
    node_state: &NodeState,
) -> Result<(Box<dyn TracingReload>, Option<agent::Agent>), NodeSetupError> {
    if let Some(socket_path) = &node_state.manytrace_socket {
        let extension = std::sync::Arc::new(TracingExtension::new());
        let agent = AgentBuilder::new(socket_path.clone())
            .register_tracing(Box::new((*extension).clone()))
            .build()
            .map_err(|e| NodeSetupError::Custom {
                kind: clap::error::ErrorKind::Io,
                msg: format!("failed to build manytrace agent: {}", e),
            })?;
        let (filter, reload_handle) = tracing_subscriber::reload::Layer::new(
            tracing_subscriber::EnvFilter::from_default_env(),
        );
        let subscriber = tracing_subscriber::Registry::default()
            .with(ManytraceLayer::new(extension))
            .with(
                FmtLayer::default()
                    .json()
                    .with_span_events(FmtSpan::NONE)
                    .with_current_span(false)
                    .with_span_list(false)
                    .with_writer(std::io::stdout)
                    .with_ansi(false)
                    .with_filter(filter),
            );

        tracing::subscriber::set_global_default(subscriber)?;
        info!("manytrace tracing enabled");
        Ok((Box::new(reload_handle), Some(agent)))
    } else {
        let (filter, reload_handle) = tracing_subscriber::reload::Layer::new(
            tracing_subscriber::EnvFilter::from_default_env(),
        );

        let subscriber = tracing_subscriber::Registry::default().with(filter).with(
            FmtLayer::default()
                .json()
                .with_span_events(FmtSpan::NONE)
                .with_current_span(false)
                .with_span_list(false)
                .with_writer(std::io::stdout)
                .with_ansi(false),
        );

        tracing::subscriber::set_global_default(subscriber)?;
        Ok((Box::new(reload_handle), None))
    }
}

async fn run(node_state: NodeState, reload_handle: Box<dyn TracingReload>) -> Result<(), ()> {
    *HALT_TIP.lock().unwrap() = node_state.node_config.maybe_fork_tip;

    let locked_epoch_validators = ValidatorsConfig::read_from_path(&node_state.validators_path)
        .unwrap_or_else(|err| {
            panic!(
                "failed to read/parse validators_path={:?}, err={:?}",
                &node_state.validators_path, err
            )
        })
        .get_locked_validator_sets(&node_state.forkpoint_config);

    let current_epoch = node_state
        .forkpoint_config
        .high_certificate
        .qc()
        .get_epoch();
    let current_round = node_state
        .forkpoint_config
        .high_certificate
        .qc()
        .get_round()
        + Round(1);
    let router: BoxUpdater<_, _> = {
        let raptor_router = build_raptorcast_router::<
            SignatureType,
            SignatureCollectionType,
            MonadMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
            VerifiedMonadMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
        >(
            node_state.node_config.clone(),
            node_state.node_config.peer_discovery,
            node_state.router_identity,
            node_state.node_config.bootstrap.clone(),
            &node_state.node_config.fullnode_dedicated.identities,
            locked_epoch_validators.clone(),
            current_epoch,
            current_round,
        )
        .await;

        #[cfg(feature = "full-node")]
        let raptor_router = monad_router_filter::FullNodeRouterFilter::new(raptor_router);

        <_ as Updater<_>>::boxed(raptor_router)
    };

    let val_set_update_interval = SeqNum(50_000); // TODO configurable

    let statesync_threshold: usize = node_state.node_config.statesync_threshold.into();

    _ = std::fs::remove_file(node_state.mempool_ipc_path.as_path());
    _ = std::fs::remove_file(node_state.control_panel_ipc_path.as_path());
    _ = std::fs::remove_file(node_state.statesync_ipc_path.as_path());

    // FIXME this is super jank... we should always just pass the 1 file in monad-node
    let mut statesync_triedb_path = node_state.triedb_path.clone();
    if let Ok(files) = std::fs::read_dir(&statesync_triedb_path) {
        let mut files: Vec<_> = files.collect();
        assert_eq!(files.len(), 1, "nothing in triedb path");
        statesync_triedb_path = files
            .pop()
            .unwrap()
            .expect("failed to read triedb path")
            .path();
    }

    let mut bootstrap_nodes = Vec::new();
    for peer_config in &node_state.node_config.bootstrap.peers {
        let peer_id = NodeId::new(peer_config.secp256k1_pubkey);
        bootstrap_nodes.push(peer_id);
    }

    // default statesync peers to bootstrap nodes if none is specified
    let state_sync_peers = if node_state.node_config.statesync.peers.is_empty() {
        bootstrap_nodes
    } else {
        node_state
            .node_config
            .statesync
            .peers
            .into_iter()
            .map(|p| NodeId::new(p.secp256k1_pubkey))
            .collect()
    };

    // TODO: use PassThruBlockPolicy and NopStateBackend for consensus only mode
    let create_block_policy = || {
        EthBlockPolicy::new(
            GENESIS_SEQ_NUM, // FIXME: MonadStateBuilder is responsible for updating this to forkpoint root if necessary
            EXECUTION_DELAY,
            node_state.node_config.chain_id,
        )
    };

    let state_backend = StateBackendThreadClient::new({
        let triedb_path = node_state.triedb_path.clone();

        move || {
            let triedb_handle =
                TriedbReader::try_new(triedb_path.as_path()).expect("triedb should exist in path");

            StateBackendCache::new(triedb_handle, SeqNum(EXECUTION_DELAY))
        }
    });

    let mut executor = ParentExecutor {
        router,
        timer: TokioTimer::default(),
        ledger: MonadBlockFileLedger::new(node_state.ledger_path),
        checkpoint: FileCheckpoint::new(node_state.forkpoint_path),
        state_root_hash: StateRootHashTriedbPoll::new(
            &node_state.triedb_path,
            &node_state.validators_path,
            val_set_update_interval,
        ),
        timestamp: TokioTimestamp::new(Duration::from_millis(5), 100, 10001),
        txpool: EthTxPoolExecutor::new(
            create_block_policy(),
            state_backend.clone(),
            EthTxPoolIpcConfig {
                bind_path: node_state.mempool_ipc_path,
                tx_batch_size: node_state.node_config.ipc_tx_batch_size as usize,
                max_queued_batches: node_state.node_config.ipc_max_queued_batches as usize,
                queued_batches_watermark: node_state.node_config.ipc_queued_batches_watermark
                    as usize,
            },
            true,
            // TODO(andr-dev): Add tx_expiry to node config
            Duration::from_secs(15),
            Duration::from_secs(5 * 60),
            node_state.chain_config,
            node_state
                .chain_config
                .get_chain_revision(
                    node_state
                        .forkpoint_config
                        .high_certificate
                        .qc()
                        .get_round(),
                )
                .chain_params()
                .proposal_gas_limit,
        )
        .expect("txpool ipc succeeds"),
        control_panel: ControlPanelIpcReceiver::new(
            node_state.control_panel_ipc_path,
            reload_handle,
            1000,
        )
        .expect("uds bind failed"),
        loopback: LoopbackExecutor::default(),
        state_sync: StateSync::<SignatureType, SignatureCollectionType>::new(
            vec![statesync_triedb_path.to_string_lossy().to_string()],
            node_state.statesync_sq_thread_cpu,
            state_sync_peers,
            node_state
                .node_config
                .statesync_max_concurrent_requests
                .into(),
            STATESYNC_REQUEST_TIMEOUT,
            STATESYNC_REQUEST_TIMEOUT,
            node_state
                .statesync_ipc_path
                .to_str()
                .expect("invalid file name")
                .to_owned(),
        ),
        config_loader: ConfigLoader::new(node_state.node_config_path),
    };

    let logger_config: WALoggerConfig<LogFriendlyMonadEvent<_, _, _>> = WALoggerConfig::new(
        node_state.wal_path.clone(), // output wal path
        false,                       // flush on every write
    );
    let Ok(mut wal) = logger_config.build() else {
        event!(
            Level::ERROR,
            path = node_state.wal_path.as_path().display().to_string(),
            "failed to initialize wal",
        );
        return Err(());
    };

    let block_sync_override_peers = node_state
        .node_config
        .blocksync_override
        .peers
        .into_iter()
        .map(|p| NodeId::new(p.secp256k1_pubkey))
        .collect();

    let mut last_ledger_tip = None;

    let builder = MonadStateBuilder {
        validator_set_factory: ValidatorSetFactory::default(),
        leader_election: WeightedRoundRobin::default(),
        block_validator: EthValidator::new(node_state.node_config.chain_id),
        block_policy: create_block_policy(),
        state_backend,
        key: node_state.secp256k1_identity,
        certkey: node_state.bls12_381_identity,
        val_set_update_interval,
        epoch_start_delay: Round(5000),
        beneficiary: node_state.node_config.beneficiary.into(),
        locked_epoch_validators,
        forkpoint: node_state.forkpoint_config.into(),
        block_sync_override_peers,
        consensus_config: ConsensusConfig {
            execution_delay: SeqNum(EXECUTION_DELAY),
            delta: Duration::from_millis(100),
            // StateSync -> Live transition happens here
            statesync_to_live_threshold: SeqNum(statesync_threshold as u64),
            // Live -> StateSync transition happens here
            live_to_statesync_threshold: SeqNum(statesync_threshold as u64 * 3 / 2),
            // Live starts execution here
            start_execution_threshold: SeqNum(statesync_threshold as u64 / 2),
            chain_config: node_state.chain_config,
            timestamp_latency_estimate_ns: 20_000_000,
            _phantom: Default::default(),
        },
        _phantom: PhantomData,
    };

    let (mut state, init_commands) = builder.build();
    executor.exec(init_commands);

    let mut ledger_span = tracing::info_span!("ledger_span", last_ledger_tip =? last_ledger_tip);

    let (maybe_otel_meter_provider, mut maybe_metrics_ticker) = node_state
        .otel_endpoint_interval
        .map(|(otel_endpoint, record_metrics_interval)| {
            let provider = build_otel_meter_provider(
                &otel_endpoint,
                format!(
                    "{network_name}_{node_name}",
                    network_name = node_state.node_config.network_name,
                    node_name = node_state.node_config.node_name
                ),
                record_metrics_interval,
            )
            .expect("failed to build otel monad-node");

            let mut timer = tokio::time::interval(record_metrics_interval);

            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            (provider, timer)
        })
        .unzip();

    let maybe_otel_meter = maybe_otel_meter_provider
        .as_ref()
        .map(|provider| provider.meter("opentelemetry"));

    let mut gauge_cache = HashMap::new();

    let mut sigterm = signal(SignalKind::terminate()).expect("in tokio rt");
    let mut sigint = signal(SignalKind::interrupt()).expect("in tokio rt");

    loop {
        tokio::select! {
            biased; // events are in order of priority

            result = sigterm.recv() => {
                info!(?result, "received SIGTERM, exiting...");
                break;
            }
            result = sigint.recv() => {
                info!(?result, "received SIGINT, exiting...");
                break;
            }
            _ = match &mut maybe_metrics_ticker {
                Some(ticker) => ticker.tick().boxed(),
                None => futures_util::future::pending().boxed(),
            } => {
                let otel_meter = maybe_otel_meter.as_ref().expect("otel_endpoint must have been set");
                let state_metrics = state.metrics();
                let executor_metrics = executor.metrics();
                send_metrics(otel_meter, &mut gauge_cache, state_metrics, executor_metrics);
            }
            event = executor.next().instrument(ledger_span.clone()) => {
                let Some(event) = event else {
                    event!(Level::ERROR, "parent executor returned none!");
                    return Err(());
                };
                let event_debug = {
                    let _timer = DropTimer::start(Duration::from_millis(1), |elapsed| {
                        warn!(
                            ?elapsed,
                            ?event,
                            "long time to format event"
                        )
                    });
                    format!("{:?}", event)
                };

                let event = LogFriendlyMonadEvent {
                    timestamp: Utc::now(),
                    event,
                };

                {
                    let _ledger_span = ledger_span.enter();
                    let _wal_event_span = tracing::trace_span!("wal_event_span").entered();
                    if let Err(err) = wal.push(&event) {
                        event!(Level::ERROR, ?err, "failed to push to wal",);
                        return Err(());
                    }
                };

                let commands = {
                    let _timer = DropTimer::start(Duration::from_millis(50), |elapsed| {
                        warn!(
                            ?elapsed,
                            event =? event_debug,
                            "long time to update event"
                        )
                    });
                    let _ledger_span = ledger_span.enter();
                    let _event_span = tracing::trace_span!("event_span", ?event.event).entered();
                    state.update(event.event)
                };

                if !commands.is_empty() {
                    let num_commands = commands.len();
                    let _timer = DropTimer::start(Duration::from_millis(50), |elapsed| {
                        warn!(
                            ?elapsed,
                            event =? event_debug,
                            num_commands,
                            "long time to execute commands"
                        )
                    });
                    let _ledger_span = ledger_span.enter();
                    let _exec_span = tracing::trace_span!("exec_span", num_commands).entered();
                    executor.exec(commands);
                }

                if let Some(ledger_tip) = executor.ledger.last_commit() {
                    if last_ledger_tip.is_none_or(|last_ledger_tip| ledger_tip > last_ledger_tip) {
                        last_ledger_tip = Some(ledger_tip);
                        ledger_span = tracing::info_span!("ledger_span", last_ledger_tip =? last_ledger_tip);
                    }
                }
            }
        }
    }

    Ok(())
}

async fn build_raptorcast_router<ST, SCT, M, OM>(
    node_config: NodeConfig<ST>,
    peer_discovery_config: PeerDiscoveryConfig<ST>,
    identity: ST::KeyPairType,
    bootstrap_nodes: NodeBootstrapConfig<ST>,
    full_nodes: &[FullNodeIdentityConfig<CertificateSignaturePubKey<ST>>],
    locked_epoch_validators: Vec<ValidatorSetDataWithEpoch<SCT>>,
    current_epoch: Epoch,
    current_round: Round,
) -> MultiRouter<ST, M, OM, MonadEvent<ST, SCT, ExecutionProtocolType>, PeerDiscovery<ST>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>>
        + Decodable
        + From<OM>
        + Send
        + Sync
        + 'static,
    OM: Encodable + Clone + Send + Sync + 'static,
{
    let bind_address = SocketAddr::new(
        IpAddr::V4(node_config.network.bind_address_host),
        node_config.network.bind_address_port,
    );
    let Some(SocketAddr::V4(name_record_address)) = resolve_domain_v4(
        &NodeId::new(identity.pubkey()),
        &peer_discovery_config.self_address,
    ) else {
        panic!(
            "Unable to resolve self address: {:?}",
            peer_discovery_config.self_address
        );
    };

    tracing::debug!(
        ?bind_address,
        ?name_record_address,
        "Monad-node ({}) starting, pid: {}",
        if cfg!(feature = "full-node") {
            "full-node"
        } else {
            "validator"
        },
        process::id()
    );

    let network_config = node_config.network;

    let mut dp_builder = DataplaneBuilder::new(&bind_address, network_config.max_mbps.into());
    if let Some(buffer_size) = network_config.buffer_size {
        dp_builder = dp_builder.with_udp_buffer_size(buffer_size);
    }

    let self_id = NodeId::new(identity.pubkey());
    let self_record = NameRecord {
        address: name_record_address,
        seq: peer_discovery_config.self_record_seq_num,
    };
    let self_record = MonadNameRecord::new(self_record, &identity);
    assert!(
        self_record.signature == peer_discovery_config.self_name_record_sig,
        "self name record signature mismatch"
    );

    // initial set of peers
    let routing_info = bootstrap_nodes
        .peers
        .iter()
        .filter_map(|peer| {
            let node_id = NodeId::new(peer.secp256k1_pubkey);
            if node_id == self_id {
                return None;
            }
            let address = match resolve_domain_v4(&node_id, &peer.address) {
                Some(SocketAddr::V4(addr)) => addr,
                _ => {
                    warn!(?node_id, ?peer.address, "Unable to resolve");
                    return None;
                }
            };
            let name_record = NameRecord {
                address,
                seq: peer.record_seq_num,
            };

            // verify signature of name record
            let mut encoded = Vec::new();
            name_record.encode(&mut encoded);
            match peer
                .name_record_sig
                .verify::<signing_domain::NameRecord>(&encoded, &peer.secp256k1_pubkey)
            {
                Ok(_) => Some((
                    node_id,
                    MonadNameRecord {
                        name_record,
                        signature: peer.name_record_sig,
                    },
                )),
                Err(_) => {
                    warn!(?node_id, "invalid name record signature in config file");
                    None
                }
            }
        })
        .collect();

    let epoch_validators = locked_epoch_validators
        .iter()
        .map(|epoch_validators| {
            (
                epoch_validators.epoch,
                epoch_validators
                    .validators
                    .0
                    .iter()
                    .map(|validator| validator.node_id)
                    .collect(),
            )
        })
        .collect();
    let mut pinned_full_nodes: BTreeSet<_> = full_nodes
        .iter()
        .map(|full_node| NodeId::new(full_node.secp256k1_pubkey))
        .collect();

    let secondary_instance: RaptorCastConfigSecondary<ST> = {
        if let Some(cfg_2nd) = node_config.fullnode_raptorcast {
            match cfg_2nd.mode {
                monad_node_config::fullnode_raptorcast::SecondaryRaptorCastModeConfig::None => {
                    debug!("Configured with Secondary RaptorCast instance: None");
                    RaptorCastConfigSecondary::default()
                }

                monad_node_config::fullnode_raptorcast::SecondaryRaptorCastModeConfig::Client => {
                    debug!("Configured with Secondary RaptorCast instance: Client");
                    RaptorCastConfigSecondary {
                        raptor10_redundancy: cfg_2nd.raptor10_fullnode_redundancy_factor,
                        mode: SecondaryRaptorCastModeConfig::Client(RaptorCastConfigSecondaryClient {
                            bandwidth_cost_per_group_member: cfg_2nd.bandwidth_cost_per_group_member,
                            bandwidth_capacity: cfg_2nd.bandwidth_capacity,
                            invite_future_dist_min: cfg_2nd.invite_future_dist_min,
                            invite_future_dist_max: cfg_2nd.invite_future_dist_max,
                            invite_accept_heartbeat: Duration::from_millis(cfg_2nd.invite_accept_heartbeat_ms),
                        })
                    }
                }

                monad_node_config::fullnode_raptorcast::SecondaryRaptorCastModeConfig::Publisher => {
                    debug!("Configured with Secondary RaptorCast instance: Publisher");
                    let full_nodes_prioritized: Vec<NodeId<CertificateSignaturePubKey<ST>>> = cfg_2nd
                        .full_nodes_prioritized
                        .identities
                        .iter()
                        .map(|id| NodeId::new(id.secp256k1_pubkey))
                        .collect();
                    // also pin these full nodes in peer discovery
                    pinned_full_nodes.extend(full_nodes_prioritized.iter());

                    RaptorCastConfigSecondary {
                        raptor10_redundancy: cfg_2nd.raptor10_fullnode_redundancy_factor,
                        mode: SecondaryRaptorCastModeConfig::Publisher(RaptorCastConfigSecondaryPublisher {
                            full_nodes_prioritized,
                            group_scheduling: GroupSchedulingConfig {
                                max_group_size: cfg_2nd.max_group_size,
                                round_span: cfg_2nd.round_span,
                                invite_lookahead: cfg_2nd.invite_lookahead,
                                max_invite_wait: cfg_2nd.max_invite_wait,
                                deadline_round_dist: cfg_2nd.deadline_round_dist,
                                init_empty_round_span: cfg_2nd.init_empty_round_span,
                            }
                        })
                    }
                }
            }
        } else {
            RaptorCastConfigSecondary::default()
        }
    };

    let peer_discovery_builder = PeerDiscoveryBuilder {
        self_id,
        self_record,
        current_round,
        current_epoch,
        epoch_validators,
        pinned_full_nodes,
        routing_info,
        ping_period: Duration::from_secs(peer_discovery_config.ping_period),
        refresh_period: Duration::from_secs(peer_discovery_config.refresh_period),
        request_timeout: Duration::from_secs(peer_discovery_config.request_timeout),
        unresponsive_prune_threshold: peer_discovery_config.unresponsive_prune_threshold,
        last_participation_prune_threshold: peer_discovery_config
            .last_participation_prune_threshold,
        min_num_peers: peer_discovery_config.min_num_peers,
        max_num_peers: peer_discovery_config.max_num_peers,
        rng: ChaCha8Rng::from_entropy(),
    };

    MultiRouter::new(
        RaptorCastConfig {
            shared_key: Arc::new(identity),
            mtu: network_config.mtu,
            udp_message_max_age_ms: network_config.udp_message_max_age_ms,
            primary_instance: RaptorCastConfigPrimary {
                raptor10_redundancy: node_config.raptor10_validator_redundancy_factor,
                fullnode_dedicated: full_nodes
                    .iter()
                    .map(|full_node| NodeId::new(full_node.secp256k1_pubkey))
                    .collect(),
            },
            secondary_instance,
        },
        dp_builder,
        peer_discovery_builder,
    )
}

fn resolve_domain_v4<P: PubKey>(node_id: &NodeId<P>, domain: &String) -> Option<SocketAddr> {
    let resolved = match domain.to_socket_addrs() {
        Ok(resolved) => resolved,
        Err(err) => {
            warn!(?node_id, ?domain, ?err, "Unable to resolve");
            return None;
        }
    };

    for entry in resolved {
        match entry {
            SocketAddr::V4(_) => return Some(entry),
            SocketAddr::V6(_) => continue,
        }
    }

    warn!(?node_id, ?domain, "No IPv4 DNS record");
    None
}

fn send_metrics(
    meter: &opentelemetry::metrics::Meter,
    gauge_cache: &mut HashMap<&'static str, opentelemetry::metrics::Gauge<u64>>,
    state_metrics: &Metrics,
    executor_metrics: ExecutorMetricsChain,
) {
    for (k, v) in state_metrics
        .metrics()
        .into_iter()
        .chain(executor_metrics.into_inner())
    {
        let gauge = gauge_cache
            .entry(k)
            .or_insert_with(|| meter.u64_gauge(k).build());
        gauge.record(v, &[]);
    }
}

fn build_otel_meter_provider(
    otel_endpoint: &str,
    service_name: String,
    interval: Duration,
) -> Result<opentelemetry_sdk::metrics::SdkMeterProvider, NodeSetupError> {
    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_timeout(interval * 2)
        .with_endpoint(otel_endpoint)
        .build()?;

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(interval / 2)
        .build();

    let provider_builder = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(
            opentelemetry_sdk::Resource::builder_empty()
                .with_attributes(vec![
                    opentelemetry::KeyValue::new(
                        opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                        service_name,
                    ),
                    opentelemetry::KeyValue::new(
                        opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
                        CLIENT_VERSION,
                    ),
                ])
                .build(),
        );

    Ok(provider_builder.build())
}
