use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, VecDeque},
    marker::PhantomData,
    net::SocketAddr,
    pin::{pin, Pin},
    task::{Context, Poll, Waker},
    time::Duration,
};

use alloy_rlp::{Decodable, Encodable};
use bytes::{Bytes, BytesMut};
use futures::{channel::oneshot, Future, FutureExt, Stream, StreamExt};
use message::{InboundRouterMessage, OutboundRouterMessage};
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_dataplane::{
    udp::segment_size_for_mtu, BroadcastMsg, DataplaneBuilder, DataplaneWriter, RecvTcpMsg,
    RecvUdpMsg, TcpMsg, TcpReader, UdpReader, UnicastMsg,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{
    ControlPanelEvent, GetFullNodes, GetPeers, Message, MonadEvent, PeerEntry, RouterCommand,
};
use monad_peer_discovery::{
    driver::{PeerDiscoveryDriver, PeerDiscoveryEmit},
    PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder, PeerDiscoveryEvent,
};
use monad_types::{DropTimer, Epoch, ExecutionProtocol, NodeId, RouterTarget};
use tokio::select;
use tracing::{debug, error, warn};

pub mod config;
pub mod message;
pub mod raptorcast_secondary;
pub mod udp;
pub mod util;
use util::{BuildTarget, EpochValidators, FullNodes, ReBroadcastGroupMap, Validator};

const SIGNATURE_SIZE: usize = 65;

pub struct RaptorCastConfig<ST, B>
where
    ST: CertificateSignatureRecoverable,
{
    pub full_nodes: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    pub peer_discovery_builder: B,

    pub key: ST::KeyPairType,
    /// amount of redundancy to send
    /// a value of 2 == send 2x total payload size total
    pub redundancy: u8,

    pub local_addr: SocketAddr,

    /// 1_000 = 1 Gbps, 10_000 = 10 Gbps
    pub up_bandwidth_mbps: u64,
    pub mtu: u16,
    pub buffer_size: Option<usize>,
}

pub struct RaptorCast<ST, M, OM, SE, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    key: ST::KeyPairType,
    redundancy: u8,

    // Raptorcast group with stake information. For the send side (i.e., initiating proposals)
    epoch_validators: BTreeMap<Epoch, EpochValidators<ST>>,
    rebroadcast_map: ReBroadcastGroupMap<ST>,

    // TODO support dynamic updating
    full_nodes: FullNodes<CertificateSignaturePubKey<ST>>,
    peer_discovery_driver: PeerDiscoveryDriver<PD>,

    current_epoch: Epoch,

    udp_state: udp::UdpState<ST>,
    mtu: u16,

    dataplane_writer: DataplaneWriter,
    dataplane_tcp_reader: TcpReader,
    dataplane_udp_reader: UdpReader,

    pending_events: VecDeque<RaptorCastEvent<M::Event, ST>>,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    _phantom: PhantomData<(OM, SE)>,
}

pub enum PeerManagerResponse<ST: CertificateSignatureRecoverable> {
    PeerList(Vec<PeerEntry<ST>>),
    FullNodes(Vec<NodeId<CertificateSignaturePubKey<ST>>>),
}

pub enum RaptorCastEvent<E, ST: CertificateSignatureRecoverable> {
    Message(E),
    PeerManagerResponse(PeerManagerResponse<ST>),
}

impl<ST, M, OM, SE, PD> RaptorCast<ST, M, OM, SE, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    pub fn new<B>(config: RaptorCastConfig<ST, B>) -> Self
    where
        B: PeerDiscoveryAlgoBuilder<PeerDiscoveryAlgoType = PD>,
    {
        assert!(config.redundancy >= 1);
        let self_id = NodeId::new(config.key.pubkey());
        let mut builder = DataplaneBuilder::new(&config.local_addr, config.up_bandwidth_mbps);
        if let Some(buffer_size) = config.buffer_size {
            builder = builder.with_udp_buffer_size(buffer_size);
        }
        let (dataplane_writer, dataplane_reader) = builder.build().split();
        let (dataplane_tcp_reader, dataplane_udp_reader) = dataplane_reader.split();
        let is_fullnode = false; // This will come from the config in upcoming PR

        Self {
            epoch_validators: Default::default(),
            rebroadcast_map: ReBroadcastGroupMap::new(self_id, is_fullnode),
            full_nodes: FullNodes::new(config.full_nodes),
            peer_discovery_driver: PeerDiscoveryDriver::new(config.peer_discovery_builder),

            key: config.key,
            redundancy: config.redundancy,

            current_epoch: Epoch(0),

            udp_state: udp::UdpState::new(self_id),
            mtu: config.mtu,

            dataplane_writer,
            dataplane_tcp_reader,
            dataplane_udp_reader,

            pending_events: Default::default(),

            waker: None,
            metrics: Default::default(),
            _phantom: PhantomData,
        }
    }

    fn tcp_build_and_send(
        &mut self,
        to: &NodeId<CertificateSignaturePubKey<ST>>,
        make_app_message: impl FnOnce() -> Bytes,
        completion: Option<oneshot::Sender<()>>,
    ) {
        match self.peer_discovery_driver.get_addr(to) {
            None => {
                warn!(?to, "not sending message, address unknown");
            }
            Some(address) => {
                let app_message = make_app_message();
                // TODO make this more sophisticated
                // include timestamp, etc
                let mut signed_message = BytesMut::zeroed(SIGNATURE_SIZE + app_message.len());
                let signature = ST::sign(&app_message, &self.key).serialize();
                assert_eq!(signature.len(), SIGNATURE_SIZE);
                signed_message[..SIGNATURE_SIZE].copy_from_slice(&signature);
                signed_message[SIGNATURE_SIZE..].copy_from_slice(&app_message);
                self.dataplane_writer.tcp_write(
                    address,
                    TcpMsg {
                        msg: signed_message.freeze(),
                        completion,
                    },
                );
            }
        };
    }

    fn handle_udp_message(&mut self, message: RecvUdpMsg) {
        // FIXME: pass dataplane as arg to handle_message
        let dataplane = RefCell::new(&mut self.dataplane_writer);
        let full_node_addrs = self
            .full_nodes
            .list
            .iter()
            .filter_map(|node_id| self.peer_discovery_driver.get_addr(node_id))
            .collect::<Vec<_>>();

        let reboradcast = |targets: Vec<_>, payload, bcast_stride| {
            // Callback for re-broadcasting raptorcast chunks to other RaptorCast participants (validator peers)
            let target_addrs: Vec<SocketAddr> = targets
                .into_iter()
                .filter_map(|target| self.peer_discovery_driver.get_addr(&target))
                .collect();

            dataplane.borrow_mut().udp_write_broadcast(BroadcastMsg {
                targets: target_addrs,
                payload,
                stride: bcast_stride,
            });
        };

        let forward = |payload, bcast_stride| {
            // callback for forwarding chunks to full nodes
            dataplane.borrow_mut().udp_write_broadcast(BroadcastMsg {
                targets: full_node_addrs.clone(),
                payload,
                stride: bcast_stride,
            });
        };

        // Enter the received raptorcast chunk into the udp_state for reassembly.
        // If the field "first-hop recipient" in the chunk has our node Id, then
        // we are responsible for broadcasting this chunk to other validators.
        // Once we have enough (redundant) raptorcast chunks, recreate the
        // decoded (AKA parsed, original) message.
        // Stream the chunks to our dedicated full-nodes as we receive them.
        let decoded_app_messages = {
            self.udp_state
                .handle_message(&self.rebroadcast_map, reboradcast, forward, message)
        };
        let mut new_events = vec![];
        for (from, decoded) in decoded_app_messages {
            match InboundRouterMessage::<M, ST>::try_deserialize(&decoded) {
                Ok(InboundRouterMessage::AppMessage(app_message)) => {
                    let event = RaptorCastEvent::Message(app_message.event(from));
                    new_events.push(event);
                }
                Ok(InboundRouterMessage::PeerDiscoveryMessage(peer_disc_message)) => {
                    // handle peer discovery message in driver
                    self.peer_discovery_driver
                        .update(peer_disc_message.event(from));
                }
                Ok(InboundRouterMessage::FullNodesGroup(_full_nodes_group_message)) => {
                    warn!("Handling of FullNodesGroup implemented in upcoming PR");
                }
                Err(err) => {
                    warn!(
                        ?from,
                        ?err,
                        decoded = hex::encode(&decoded),
                        "failed to deserialize message"
                    );
                }
            }
        }

        self.pending_events.extend(new_events);
    }

    fn handle_tcp_message(&mut self, message: RecvTcpMsg) {
        let RecvTcpMsg { src_addr, payload } = message;
        // check message length to prevent panic during message slicing
        if payload.len() < SIGNATURE_SIZE {
            warn!(
                ?src_addr,
                "invalid message, message length less than signature size"
            );
            return;
        }

        let signature_bytes = &payload[..SIGNATURE_SIZE];
        let signature = match ST::deserialize(signature_bytes) {
            Ok(signature) => signature,
            Err(err) => {
                warn!(?err, ?src_addr, "invalid signature");
                return;
            }
        };
        let app_message_bytes = payload.slice(SIGNATURE_SIZE..);
        let deserialized_message =
            match InboundRouterMessage::<M, ST>::try_deserialize(&app_message_bytes) {
                Ok(message) => message,
                Err(err) => {
                    warn!(?err, ?src_addr, "failed to deserialize message");
                    return;
                }
            };
        let from = match signature.recover_pubkey(app_message_bytes.as_ref()) {
            Ok(from) => from,
            Err(err) => {
                warn!(?err, ?src_addr, "failed to recover pubkey");
                return;
            }
        };

        // Dispatch messages received via TCP
        match deserialized_message {
            InboundRouterMessage::AppMessage(message) => {
                let event = RaptorCastEvent::Message(message.event(NodeId::new(from))).into();
                self.pending_events.push_back(event);
            }
            InboundRouterMessage::PeerDiscoveryMessage(message) => {
                // peer discovery message should come through udp
                debug!(
                    ?message,
                    "dropping peer discovery message, should come through udp channel"
                );
            }
            InboundRouterMessage::FullNodesGroup(_group_message) => {
                // pass TCP message to MultiRouter
                warn!("FullNodesGroup protocol via TCP not implemented");
            }
        }
    }

    fn handle_peer_discovery_emit(&mut self, peer_disc_emit: PeerDiscoveryEmit<ST>) {
        match peer_disc_emit {
            PeerDiscoveryEmit::RouterCommand { target, message } => {
                let router_message =
                    OutboundRouterMessage::serialize(
                        OutboundRouterMessage::<OM, ST>::PeerDiscoveryMessage(message),
                    );
                let current_epoch = self.current_epoch;
                let unicast_msg = udp_build(
                    &current_epoch,
                    BuildTarget::<ST>::PointToPoint(&target),
                    router_message,
                    self.mtu,
                    &self.key,
                    self.redundancy,
                    &self.peer_discovery_driver.get_known_addresses(),
                );
                self.dataplane_writer.udp_write_unicast(unicast_msg);
            }
        }
    }

    // returns false if there is no more pending messages
    async fn process_next_message(&mut self) -> bool
    where
        PeerDiscoveryDriver<PD>: Unpin,
    {
        select! {
            msg = self.dataplane_tcp_reader.read() => {
                self.handle_tcp_message(msg);
                true
            },
            msg = self.dataplane_udp_reader.read() => {
                self.handle_udp_message(msg);
                true
            },
            Some(emit) = self.peer_discovery_driver.next() => {
                self.handle_peer_discovery_emit(emit);
                true
            },
            else => false,
        }
    }
}

impl<ST, M, OM, SE, PD> Executor for RaptorCast<ST, M, OM, SE, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    type Command = RouterCommand<ST, OM>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let self_id = NodeId::new(self.key.pubkey());
        for command in commands {
            match command {
                RouterCommand::UpdateCurrentRound(epoch, round) => {
                    assert!(epoch >= self.current_epoch);
                    self.current_epoch = epoch;
                    self.rebroadcast_map.delete_expired_groups(epoch, round);
                    while let Some(entry) = self.epoch_validators.first_entry() {
                        if *entry.key() + Epoch(1) < self.current_epoch {
                            entry.remove();
                        } else {
                            break;
                        }
                    }
                    self.peer_discovery_driver
                        .update(PeerDiscoveryEvent::UpdateCurrentEpoch { epoch });
                }
                RouterCommand::AddEpochValidatorSet {
                    epoch,
                    validator_set,
                } => {
                    self.rebroadcast_map
                        .push_group_validator_set(validator_set.clone(), epoch);
                    if let Some(epoch_validators) = self.epoch_validators.get(&epoch) {
                        assert_eq!(validator_set.len(), epoch_validators.validators.len());
                        assert!(validator_set.clone().into_iter().all(
                            |(validator_key, validator_stake)| epoch_validators
                                .validators
                                .get(&validator_key)
                                .map(|v| v.stake)
                                == Some(validator_stake)
                        ));
                        warn!("duplicate validator set update (this is safe but unexpected)")
                    } else {
                        let removed = self.epoch_validators.insert(
                            epoch,
                            EpochValidators {
                                validators: validator_set
                                    .clone()
                                    .into_iter()
                                    .map(|(validator_key, validator_stake)| {
                                        (
                                            validator_key,
                                            Validator {
                                                stake: validator_stake,
                                            },
                                        )
                                    })
                                    .collect(),
                            },
                        );
                        assert!(removed.is_none());
                    }
                    self.peer_discovery_driver
                        .update(PeerDiscoveryEvent::UpdateValidatorSet {
                            epoch,
                            validators: validator_set.into_iter().map(|(id, _)| id).collect(),
                        });
                }
                RouterCommand::Publish { target, message } => {
                    let _timer = DropTimer::start(Duration::from_millis(20), |elapsed| {
                        warn!(?elapsed, "long time to publish message")
                    });

                    // TODO: perhaps pass this directly to udp_build to avoid calling on every exec
                    let known_addresses = self.peer_discovery_driver.get_known_addresses();

                    // send message to self if applicable
                    match target {
                        RouterTarget::Broadcast(epoch) | RouterTarget::Raptorcast(epoch) => {
                            let Some(epoch_validators) = self.epoch_validators.get_mut(&epoch)
                            else {
                                error!(
                                    "don't have epoch validators populated for epoch: {:?}",
                                    epoch
                                );
                                continue;
                            };

                            if epoch_validators.validators.contains_key(&self_id) {
                                let message: M = message.clone().into();
                                self.pending_events
                                    .push_back(RaptorCastEvent::Message(message.event(self_id)));
                                if let Some(waker) = self.waker.take() {
                                    waker.wake()
                                }
                            }
                            let epoch_validators_without_self =
                                epoch_validators.view_without(vec![&self_id]);
                            let full_nodes_view = self.full_nodes.view();

                            if epoch_validators_without_self.view().is_empty()
                                && full_nodes_view.view().is_empty()
                            {
                                // this is degenerate case where the only
                                // validator is self and we have no full nodes
                                // to forward
                                continue;
                            }

                            let build_target = match &target {
                                RouterTarget::Broadcast(_) => {
                                    BuildTarget::Broadcast(epoch_validators_without_self)
                                }
                                RouterTarget::Raptorcast(_) => BuildTarget::Raptorcast((
                                    epoch_validators_without_self,
                                    full_nodes_view,
                                )),
                                _ => unreachable!(),
                            };
                            let outbound_message =
                                OutboundRouterMessage::<OM, ST>::AppMessage(message).serialize();
                            let unicast_msg = udp_build(
                                &epoch,
                                build_target,
                                outbound_message,
                                self.mtu,
                                &self.key,
                                self.redundancy,
                                &known_addresses,
                            );
                            self.dataplane_writer.udp_write_unicast(unicast_msg);
                        }
                        RouterTarget::PointToPoint(to) => {
                            if to == self_id {
                                let message: M = message.into();
                                self.pending_events
                                    .push_back(RaptorCastEvent::Message(message.event(self_id)));
                                if let Some(waker) = self.waker.take() {
                                    waker.wake()
                                }
                            } else {
                                let outbound_message =
                                    OutboundRouterMessage::<OM, ST>::AppMessage(message)
                                        .serialize();
                                let unicast_msg = udp_build(
                                    &self.current_epoch,
                                    BuildTarget::<ST>::PointToPoint(&to),
                                    outbound_message,
                                    self.mtu,
                                    &self.key,
                                    self.redundancy,
                                    &known_addresses,
                                );
                                self.dataplane_writer.udp_write_unicast(unicast_msg);
                            }
                        }
                        RouterTarget::TcpPointToPoint { to, completion } => {
                            if to == self_id {
                                let message: M = message.into();
                                self.pending_events
                                    .push_back(RaptorCastEvent::Message(message.event(self_id)));
                                if let Some(waker) = self.waker.take() {
                                    waker.wake()
                                }
                            } else {
                                let app_message =
                                    OutboundRouterMessage::<OM, ST>::AppMessage(message);
                                self.tcp_build_and_send(&to, || app_message.serialize(), completion)
                            }
                        }
                    };
                }
                RouterCommand::PublishToFullNodes { .. } => {}
                RouterCommand::GetPeers => {
                    let name_records = self.peer_discovery_driver.get_name_records();
                    let peer_list = name_records
                        .iter()
                        .map(|(node_id, name_record)| PeerEntry {
                            pubkey: node_id.pubkey(),
                            addr: name_record.address(),
                            signature: name_record.signature,
                            record_seq_num: name_record.seq(),
                        })
                        .collect::<Vec<_>>();
                    self.pending_events
                        .push_back(RaptorCastEvent::PeerManagerResponse(
                            PeerManagerResponse::PeerList(peer_list),
                        ));
                }
                RouterCommand::UpdatePeers(peers) => {
                    self.peer_discovery_driver
                        .update(PeerDiscoveryEvent::UpdatePeers { peers });
                }
                RouterCommand::GetFullNodes => {
                    let full_nodes = self.full_nodes.list.clone();
                    self.pending_events
                        .push_back(RaptorCastEvent::PeerManagerResponse(
                            PeerManagerResponse::FullNodes(full_nodes),
                        ));
                }
                RouterCommand::UpdateFullNodes(new_full_nodes) => {
                    self.full_nodes.list = new_full_nodes;
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        ExecutorMetricsChain::default()
            .push(self.metrics.as_ref())
            .push(self.peer_discovery_driver.metrics())
    }
}

fn udp_build<ST: CertificateSignatureRecoverable>(
    epoch: &Epoch,
    build_target: BuildTarget<ST>,
    outbound_message: Bytes,
    mtu: u16,
    key: &ST::KeyPairType,
    redundancy: u8,
    known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
) -> UnicastMsg {
    let segment_size = segment_size_for_mtu(mtu);

    let unix_ts_ms = std::time::UNIX_EPOCH
        .elapsed()
        .expect("time went backwards")
        .as_millis()
        .try_into()
        .expect("unix epoch doesn't fit in u64");

    let messages = udp::build_messages::<ST>(
        key,
        segment_size,
        outbound_message,
        redundancy,
        epoch.0,
        unix_ts_ms,
        build_target,
        known_addresses,
    );

    UnicastMsg {
        msgs: messages,
        stride: segment_size,
    }
}

impl<ST, M, OM, E, PD> Stream for RaptorCast<ST, M, OM, E, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    E: From<RaptorCastEvent<M::Event, ST>>,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    PeerDiscoveryDriver<PD>: Unpin,
    Self: Unpin,
{
    type Item = E;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.waker.is_none() {
            self.waker = Some(cx.waker().clone());
        }

        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(Some(event.into()));
        }

        while let Poll::Ready(true) = pin!(self.process_next_message()).poll(cx) {}

        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(Some(event.into()));
        }

        Poll::Pending
    }
}

impl<ST, SCT, EPT> From<RaptorCastEvent<MonadEvent<ST, SCT, EPT>, ST>> for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(value: RaptorCastEvent<MonadEvent<ST, SCT, EPT>, ST>) -> Self {
        match value {
            RaptorCastEvent::Message(event) => event,
            RaptorCastEvent::PeerManagerResponse(peer_manager_response) => {
                match peer_manager_response {
                    PeerManagerResponse::PeerList(peer) => MonadEvent::ControlPanelEvent(
                        ControlPanelEvent::GetPeers(GetPeers::Response(peer)),
                    ),
                    PeerManagerResponse::FullNodes(full_nodes) => MonadEvent::ControlPanelEvent(
                        ControlPanelEvent::GetFullNodes(GetFullNodes::Response(full_nodes)),
                    ),
                }
            }
        }
    }
}
