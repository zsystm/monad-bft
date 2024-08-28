use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    marker::PhantomData,
    net::SocketAddr,
    num::NonZero,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use lru::LruCache;
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_dataplane::event_loop::{Dataplane, UnicastMsg};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{Message, RouterCommand};
use monad_raptor::{ManagedDecoder, SOURCE_SYMBOLS_MIN};
use monad_types::{Deserializable, DropTimer, Epoch, NodeId, RouterTarget, Serializable};

pub mod udp;
pub mod util;
use util::{compute_hash, BuildTarget, EpochValidators, Validator};

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

pub const PENDING_MESSAGE_CACHE_SIZE: NonZero<usize> = unsafe { NonZero::new_unchecked(1_000) };

pub const SIGNATURE_CACHE_SIZE: NonZero<usize> = unsafe { NonZero::new_unchecked(10_000) };
pub const RECENTLY_DECODED_CACHE_SIZE: NonZero<usize> = unsafe { NonZero::new_unchecked(10_000) };

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

    // TODO add a cap on max number of chunks that will be forwarded per message? so that a DOS
    // can't be induced by spamming broadcast chunks to any given node
    // TODO we also need to cap the max number chunks that are decoded - because an adversary could
    // generate a bunch of linearly dependent chunks and cause unbounded memory usage.
    // TODO strong bound on max amount of memory used per decoder?
    // TODO make eviction more sophisticated than LRU - should look at unix_ts_ms as well
    pending_message_cache:
        LruCache<MessageCacheKey<CertificateSignaturePubKey<ST>>, ManagedDecoder>,
    signature_cache:
        LruCache<[u8; udp::HEADER_LEN as usize - 65 + 20], NodeId<CertificateSignaturePubKey<ST>>>,
    /// Value in this map represents the # of excess chunks received for a successfully decoded msg
    recently_decoded_cache: LruCache<MessageCacheKey<CertificateSignaturePubKey<ST>>, usize>,

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
        let dataplane = Dataplane::new(&config.local_addr);
        Self {
            epoch_validators: Default::default(),
            known_addresses: config.known_addresses,

            key: config.key,
            redundancy: config.redundancy,

            current_epoch: Epoch(0),

            pending_message_cache: LruCache::unbounded(),
            signature_cache: LruCache::new(SIGNATURE_CACHE_SIZE),
            recently_decoded_cache: LruCache::new(RECENTLY_DECODED_CACHE_SIZE),

            dataplane,
            pending_events: Default::default(),

            waker: None,
            metrics: Default::default(),
            _phantom: PhantomData,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
struct MessageCacheKey<PT>
where
    PT: PubKey,
{
    unix_ts_ms: u64,
    author: NodeId<PT>,
    app_message_hash: [u8; 20],
    app_message_len: usize,
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

        while this.pending_message_cache.len() > PENDING_MESSAGE_CACHE_SIZE.into() {
            let (key, decoder) = this.pending_message_cache.pop_lru().expect("nonempty");
            tracing::warn!(
                num_source_symbols = decoder.num_source_symbols(),
                num_encoded_symbols_received = decoder.num_encoded_symbols_received(),
                inactivation_symbol_threshold = decoder.inactivation_symbol_threshold(),
                ?key,
                "dropped unfinished ManagedDecoder"
            )
        }

        let self_id = NodeId::new(this.key.pubkey());

        while let Poll::Ready(Some(message)) = this.dataplane.poll_next_unpin(cx) {
            let mut broadcast_batcher =
                udp::BroadcastBatcher::new(&mut this.dataplane, &message.payload);

            for payload_start_idx in (0..message.payload.len()).step_by(message.stride) {
                let mut batch_guard = broadcast_batcher.create_flush_guard();

                let payload_end_idx = payload_start_idx + message.stride.min(message.payload.len());
                let payload = message.payload.slice(payload_start_idx..payload_end_idx);
                let parsed_message =
                    match udp::parse_message::<ST>(&mut this.signature_cache, payload) {
                        Ok(message) => message,
                        Err(err) => {
                            tracing::warn!(?err, "unable to parse message");
                            continue;
                        }
                    };

                let self_hash = compute_hash(&self_id);
                if parsed_message.broadcast {
                    let Some(epoch_validators) =
                        this.epoch_validators.get_mut(&Epoch(parsed_message.epoch))
                    else {
                        tracing::error!(
                            epoch =? parsed_message.epoch,
                            "don't have epoch validators populated",
                        );
                        continue;
                    };
                    if !epoch_validators
                        .validators
                        .contains_key(&parsed_message.author)
                    {
                        tracing::error!(
                            author =? parsed_message.author,
                            "not in validator set"
                        );
                        continue;
                    }
                    if self_hash == parsed_message.recipient_hash {
                        let targets =
                            epoch_validators.view_without(vec![&parsed_message.author, &self_id]);
                        batch_guard.queue_broadcast(
                            payload_start_idx,
                            payload_end_idx,
                            &parsed_message.author,
                            || {
                                targets
                                    .view()
                                    .keys()
                                    .filter_map(|validator| {
                                        this.known_addresses.get(validator).copied()
                                    })
                                    .collect()
                            },
                        )
                    }
                } else if self_hash != parsed_message.recipient_hash {
                    tracing::error!("dropping spoofed message");
                    continue;
                }

                let app_message_len: usize = parsed_message.app_message_len.try_into().unwrap();
                let key = MessageCacheKey {
                    unix_ts_ms: parsed_message.unix_ts_ms,
                    author: parsed_message.author,
                    app_message_hash: parsed_message.app_message_hash,
                    app_message_len,
                };

                if let Some(excess_chunk_count) = this.recently_decoded_cache.get_mut(&key) {
                    // already decoded
                    *excess_chunk_count += 1;
                    continue;
                }

                let decoder = this.pending_message_cache.get_or_insert_mut(key, || {
                    let symbol_len = parsed_message.chunk.len();

                    // data_size is always greater than zero, so this division is safe
                    let num_source_symbols =
                        app_message_len.div_ceil(symbol_len).max(SOURCE_SYMBOLS_MIN);

                    // TODO: verify unwrap
                    ManagedDecoder::new(num_source_symbols, symbol_len).unwrap()
                });

                // can we assert!(!decoder.decoding_done()) ?

                decoder
                    .received_encoded_symbol(&parsed_message.chunk, parsed_message.chunk_id.into());

                if decoder.try_decode() {
                    let Some(mut decoded) = decoder.reconstruct_source_data() else {
                        tracing::error!("failed to reconstruct source data");
                        continue;
                    };

                    if app_message_len > 10_000 {
                        tracing::debug!(app_message_len, "reconstructed large message");
                    }

                    decoded.truncate(app_message_len);
                    // successfully decoded, so pop out from pending_messages
                    this.pending_message_cache.pop(&key);
                    this.recently_decoded_cache.push(key, 0);

                    let Ok(message) = M::deserialize(&Bytes::from(decoded)) else {
                        tracing::error!("failed to deserialize message");
                        continue;
                    };
                    this.pending_events
                        .push_back(message.event(parsed_message.author));
                }
            }
            if let Some(event) = this.pending_events.pop_front() {
                return Poll::Ready(Some(event));
            }
        }

        Poll::Pending
    }
}
