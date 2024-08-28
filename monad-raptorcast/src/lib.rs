use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    marker::PhantomData,
    net::SocketAddr,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_dataplane::event_loop::{BroadcastMsg, Dataplane, UnicastMsg};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{Message, RouterCommand};
use monad_types::{Deserializable, DropTimer, Epoch, NodeId, RouterTarget, Serializable};

pub mod udp;
pub mod util;
use util::{BuildTarget, EpochValidators, Validator};

pub struct RaptorCastConfig<ST>
where
    ST: CertificateSignatureRecoverable,
{
    // TODO support dynamic updating
    pub known_addresses: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,

    pub key: ST::KeyPairType,
    /// amount of redundancy to send
    /// a value of 2 == send 2x total payload size total
    pub redundancy: u8,

    pub local_addr: String,
}

pub struct RaptorCast<ST, M, OM>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes>,
    OM: Serializable<Bytes> + Into<M> + Clone,
{
    key: ST::KeyPairType,
    redundancy: u8,

    epoch_validators: BTreeMap<Epoch, EpochValidators<ST>>,
    known_addresses: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,

    current_epoch: Epoch,

    udp_state: udp::UdpState<ST>,

    dataplane: Dataplane,
    pending_events: VecDeque<M::Event>,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    _phantom: PhantomData<OM>,
}

impl<ST, M, OM> RaptorCast<ST, M, OM>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes>,
    OM: Serializable<Bytes> + Into<M> + Clone,
{
    pub fn new(config: RaptorCastConfig<ST>) -> Self {
        let self_id = NodeId::new(config.key.pubkey());
        let dataplane = Dataplane::new(&config.local_addr);
        Self {
            epoch_validators: Default::default(),
            known_addresses: config.known_addresses,

            key: config.key,
            redundancy: config.redundancy,

            current_epoch: Epoch(0),

            udp_state: udp::UdpState::new(self_id),

            dataplane,
            pending_events: Default::default(),

            waker: None,
            metrics: Default::default(),
            _phantom: PhantomData,
        }
    }
}

impl<ST, M, OM> Executor for RaptorCast<ST, M, OM>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes>,
    OM: Serializable<Bytes> + Into<M> + Clone,
{
    type Command = RouterCommand<CertificateSignaturePubKey<ST>, OM>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let self_id = NodeId::new(self.key.pubkey());
        for command in commands {
            match command {
                RouterCommand::UpdateCurrentRound(epoch, _round) => {
                    assert!(epoch >= self.current_epoch);
                    self.current_epoch = epoch;
                    while let Some(entry) = self.epoch_validators.first_entry() {
                        if *entry.key() + Epoch(1) < self.current_epoch {
                            entry.remove();
                        } else {
                            break;
                        }
                    }
                }
                RouterCommand::AddEpochValidatorSet {
                    epoch,
                    validator_set,
                } => {
                    if let Some(epoch_validators) = self.epoch_validators.get(&epoch) {
                        assert_eq!(validator_set.len(), epoch_validators.validators.len());
                        assert!(validator_set.into_iter().all(
                            |(validator_key, validator_stake)| epoch_validators
                                .validators
                                .get(&validator_key)
                                .map(|v| v.stake)
                                == Some(validator_stake)
                        ));
                        tracing::warn!(
                            "duplicate validator set update (this is safe but unexpected)"
                        )
                    } else {
                        let removed = self.epoch_validators.insert(
                            epoch,
                            EpochValidators {
                                validators: validator_set
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
                }
                RouterCommand::Publish { target, message } => {
                    let app_message = message.serialize();
                    let app_message_len = app_message.len();
                    let _timer = DropTimer::start(Duration::from_millis(20), |elapsed| {
                        tracing::warn!(?elapsed, app_message_len, "long time to publish message")
                    });
                    // send message to self if applicable
                    let (epoch, build_target) = match &target {
                        RouterTarget::Broadcast(epoch) => {
                            let Some(epoch_validators) = self.epoch_validators.get_mut(epoch)
                            else {
                                tracing::error!(
                                    "don't have epoch validators populated for epoch: {:?}",
                                    epoch
                                );
                                continue;
                            };

                            if epoch_validators.validators.contains_key(&self_id) {
                                let message: M = message.into();
                                self.pending_events.push_back(message.event(self_id));
                                if let Some(waker) = self.waker.take() {
                                    waker.wake()
                                }
                            }
                            let epoch_validators_without_self =
                                epoch_validators.view_without(vec![&self_id]);
                            if epoch_validators_without_self.view().is_empty() {
                                // this is degenerate case where the only validator is self
                                continue;
                            }

                            (epoch, BuildTarget::Broadcast(epoch_validators_without_self))
                        }
                        RouterTarget::Raptorcast(epoch) => {
                            let Some(epoch_validators) = self.epoch_validators.get_mut(epoch)
                            else {
                                tracing::error!(
                                    "don't have epoch validators populated for epoch: {:?}",
                                    epoch
                                );
                                continue;
                            };

                            if epoch_validators.validators.contains_key(&self_id) {
                                let message: M = message.into();
                                self.pending_events.push_back(message.event(self_id));
                                if let Some(waker) = self.waker.take() {
                                    waker.wake()
                                }
                            }
                            let epoch_validators_without_self =
                                epoch_validators.view_without(vec![&self_id]);
                            if epoch_validators_without_self.view().is_empty() {
                                // this is degenerate case where the only validator is self
                                continue;
                            }

                            (
                                epoch,
                                BuildTarget::Raptorcast(epoch_validators_without_self),
                            )
                        }
                        RouterTarget::PointToPoint(to) => {
                            if to == &self_id {
                                let message: M = message.into();
                                self.pending_events.push_back(message.event(self_id));
                                if let Some(waker) = self.waker.take() {
                                    waker.wake()
                                }
                                continue;
                            } else {
                                (&self.current_epoch, BuildTarget::PointToPoint(to))
                            }
                        }
                    };

                    let unix_ts_ms = std::time::UNIX_EPOCH
                        .elapsed()
                        .expect("time went backwards")
                        .as_millis()
                        .try_into()
                        .expect("unix epoch doesn't fit in u64");
                    let messages = udp::build_messages::<ST>(
                        &self.key,
                        app_message,
                        self.redundancy,
                        epoch.0,
                        unix_ts_ms,
                        build_target,
                        &self.known_addresses,
                    );

                    self.dataplane.unicast(UnicastMsg { msgs: messages });
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, M, OM> Stream for RaptorCast<ST, M, OM>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes>,
    OM: Serializable<Bytes> + Into<M> + Clone,

    Self: Unpin,
{
    type Item = M::Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if let Some(event) = this.pending_events.pop_front() {
            return Poll::Ready(Some(event));
        }

        while let Poll::Ready(Some(message)) = this.dataplane.poll_next_unpin(cx) {
            let decoded_app_messages = this.udp_state.handle_message(
                &mut this.epoch_validators,
                |targets, payload| {
                    // this is the callback used for rebroadcasting
                    let targets = targets
                        .into_iter()
                        .filter_map(|validator| this.known_addresses.get(&validator).copied())
                        .collect();
                    this.dataplane.broadcast(BroadcastMsg { targets, payload })
                },
                message,
            );
            let deserialized_app_messages = decoded_app_messages.into_iter().filter_map(
                |(from, decoded)| match M::deserialize(&decoded) {
                    Ok(app_message) => Some(app_message.event(from)),
                    Err(_) => {
                        tracing::error!("failed to deserialize message");
                        None
                    }
                },
            );
            this.pending_events.extend(deserialized_app_messages);
            if let Some(event) = this.pending_events.pop_front() {
                return Poll::Ready(Some(event));
            }
        }

        Poll::Pending
    }
}
