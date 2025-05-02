use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{DefaultHasher, Hash, Hasher},
    time::{Duration, Instant},
};

use monad_blocktimestamp::timestamp_command::BlockTimestampCommand;

use monad_consensus_types::{
    quorum_certificate::{TimestampAdjustment, TimestampAdjustmentDirection},
    signature_collection::SignatureCollection,
    validator_data::ValidatorData,
};
use monad_crypto::certificate_signature::PubKey;
use monad_types::{Epoch, NodeId, PingSequence, Round};
use monad_executor_glue::BlockTimestampEvent;
use tracing::debug;

const MAX_LATENCY_SAMPLES: usize = 100;

const PING_PERIOD_SEC: usize = 30;

pub const PING_TICK_DURATION: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Invalid,     // timestamp did not increment compared to previous block
    OutOfBounds, // timestamp is out of bounds compared to local time
}

#[derive(Debug, Clone)]
struct RunningAverage {
    sum: Duration,
    max_samples: usize,
    samples: Vec<Duration>,
    avg: Option<Duration>,
    next_index: usize,
}

impl RunningAverage {
    pub fn new(max_samples: usize) -> Self {
        assert!(max_samples > 0);
        Self {
            sum: Default::default(),
            max_samples,
            samples: Vec::new(),
            avg: None,
            next_index: 0,
        }
    }

    pub fn add_sample(&mut self, sample: Duration) {
        if self.samples.len() == self.max_samples {
            self.sum -= self.samples[self.next_index];
            self.samples[self.next_index] = sample;
            self.next_index = (self.next_index + 1) % self.max_samples;
        } else {
            self.samples.push(sample);
        }
        self.sum += sample;
        self.avg = Some(self.sum / self.samples.len() as u32);
    }

    pub fn get_avg(&self) -> Option<Duration> {
        self.avg
    }
}

/// Ping state per validator
#[derive(Debug, Clone)]
pub struct ValidatorPingState {
    sequence: PingSequence,       // sequence number of the last ping sent
    ping_latency: RunningAverage, // average latency of the last pings
    last_ping_time: Instant,      // time of the last ping sent
}

impl Default for ValidatorPingState {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            sequence: PingSequence(0),
            ping_latency: RunningAverage::new(MAX_LATENCY_SAMPLES),
            last_ping_time: now,
        }
    }
}

impl ValidatorPingState {
    pub fn new() -> Self {
        Self::default()
    }
    // initiate a new ping
    pub fn start_next(&mut self) -> PingSequence {
        self.sequence.0 = self.sequence.0.wrapping_add(1);
        self.last_ping_time = Instant::now();
        self.sequence
    }

    pub fn pong_received(&mut self, sequence: PingSequence) -> Option<Duration> {
        if sequence != self.sequence {
            return None;
        }
        // estimate latency as half the round trip time
        let elapsed = self.last_ping_time.elapsed() / 2;
        self.update_latency(elapsed);
        Some(elapsed)
    }

    pub fn avg_latency(&self) -> Option<Duration> {
        self.ping_latency.get_avg()
    }

    fn update_latency(&mut self, latency: Duration) {
        self.ping_latency.add_sample(latency);
    }
}

#[derive(Debug, Clone)]
struct PingState<P: PubKey> {
    current_epoch: Epoch,
    epoch_validators: BTreeMap<Epoch, Vec<NodeId<P>>>,
    validators: BTreeMap<NodeId<P>, ValidatorPingState>, // a union of the current and the next epoch validator sets
    schedule: Box<[Vec<NodeId<P>>; PING_PERIOD_SEC]>,    // schedule of validators to ping
    tick: usize,                                         // current tick index into schedule
}

impl<P: PubKey> PingState<P> {
    pub fn new() -> Self {
        Self {
            current_epoch: Epoch(0),
            epoch_validators: BTreeMap::new(),
            validators: BTreeMap::new(),
            schedule: Box::new([const { Vec::new() }; PING_PERIOD_SEC]),
            tick: 0,
        }
    }

    pub fn pong_received(&mut self, node_id: NodeId<P>, sequence: PingSequence) {
        if let Some(validator_state) = self.validators.get_mut(&node_id) {
            if let Some(elapsed) = validator_state.pong_received(sequence) {
                debug!(?node_id, elapsed_ms = ?elapsed.as_millis(), avg_latency_ms = ?validator_state.avg_latency().unwrap_or_default().as_millis(), "ping latency");
            }
        }
    }

    fn update_validators<SCT>(
        &mut self,
        validators: &[ValidatorData<SCT>],
        my_node: &NodeId<P>,
        val_epoch: &Epoch,
    ) where
        SCT: SignatureCollection<NodeIdPubKey = P>,
    {
        match self.epoch_validators.entry(*val_epoch) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(
                    validators
                        .iter()
                        .filter(|v| v.node_id != *my_node)
                        .map(|v| v.node_id)
                        .collect(),
                );
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                let val_set: Vec<_> = validators
                    .iter()
                    .filter(|v| v.node_id != *my_node)
                    .map(|v| v.node_id)
                    .collect();
                assert_eq!(
                    *entry.get(),
                    val_set,
                    "Validators update is not matching existing for the same epoch"
                )
            }
        }
    }

    fn compute_schedule(&mut self) {
        let active_validators = self
            .epoch_validators
            .iter()
            .filter(|entry| {
                *entry.0 == self.current_epoch || *entry.0 == self.current_epoch + Epoch(1)
            })
            .flat_map(|entry| entry.1)
            .cloned()
            .collect::<BTreeSet<NodeId<P>>>();

        self.validators
            .retain(|node_id, _| active_validators.contains(node_id));

        for s in self.schedule.iter_mut() {
            s.clear();
        }

        // TODO: reimplement shuffling with rand.
        for validator in active_validators.iter() {
            self.validators.entry(*validator).or_default();
            let mut hasher = DefaultHasher::new();
            validator.hash(&mut hasher);
            let idx = (hasher.finish() as usize) % self.schedule.len();
            self.schedule[idx].push(*validator);
        }
    }

    // returns list of nodes to send pings to on this tick
    fn tick(&mut self) -> Vec<(NodeId<P>, PingSequence)> {
        let mut pings = Vec::new();
        for node_id in self.schedule[self.tick].iter() {
            let validator_state = self.validators.get_mut(node_id).unwrap();
            let sequence = validator_state.start_next();
            pings.push((*node_id, sequence));
        }
        self.tick = (self.tick + 1) % self.schedule.len();
        pings
    }

    fn get_latency(&self, node_id: &NodeId<P>) -> Option<Duration> {
        self.validators.get(node_id).and_then(|v| v.avg_latency())
    }
}

#[derive(Debug)]
struct SentVote {
    round: Round,
    timestamp: Instant,
}
#[derive(Debug)]
pub struct BlockTimestamp<P: PubKey> {
    local_time_ns: u128,

    max_delta_ns: u128,

    ping_state: PingState<P>,

    last_sent_vote: Option<SentVote>, // last voted round and timestamp

    default_latency_estimate_ns: u128,
}

impl<P: PubKey> BlockTimestamp<P> {
    pub fn new(max_delta_ns: u128, default_latency_estimate_ns: u128) -> Self {
        assert!(default_latency_estimate_ns > 0);
        Self {
            local_time_ns: 0,
            max_delta_ns,
            default_latency_estimate_ns,
            ping_state: PingState::new(),
            last_sent_vote: None,
        }
    }

    pub fn update_time(&mut self, time: u128) {
        self.local_time_ns = time;
    }

    pub fn get_current_time(&self) -> u128 {
        self.local_time_ns
    }

    pub fn get_valid_block_timestamp(&self, prev_block_ts: u128) -> u128 {
        if self.local_time_ns <= prev_block_ts {
            prev_block_ts + 1
        } else {
            self.local_time_ns
        }
    }

    fn valid_bounds(&self, timestamp: u128, vote_delay_ns: u128) -> bool {
        let max_delta_ns = self.max_delta_ns.saturating_add(vote_delay_ns);

        let lower_bound = self.local_time_ns.saturating_sub(max_delta_ns);
        let upper_bound = self.local_time_ns.saturating_add(max_delta_ns);

        lower_bound <= timestamp && timestamp <= upper_bound
    }

    pub fn valid_block_timestamp(
        &self,
        prev_block_ts: u128,
        curr_block_ts: u128,
        vote_delay_ns: u128,
        author: &NodeId<P>,
    ) -> Result<Option<TimestampAdjustment>, Error> {
        if curr_block_ts <= prev_block_ts {
            // block timestamp must be strictly monotonically increasing
            return Err(Error::Invalid);
        }
        if !self.valid_bounds(curr_block_ts, vote_delay_ns) {
            return Err(Error::OutOfBounds);
        }

        // adjust for estimated latency
        let latency = self
            .ping_state
            .get_latency(author)
            .unwrap_or(Duration::from_nanos(
                self.default_latency_estimate_ns.try_into().unwrap(),
            ));

        let expected_block_ts = self.local_time_ns.saturating_sub(latency.as_nanos());

        if curr_block_ts > expected_block_ts {
            Ok(Some(TimestampAdjustment {
                delta: curr_block_ts - expected_block_ts,
                direction: TimestampAdjustmentDirection::Forward,
            }))
        } else {
            Ok(Some(TimestampAdjustment {
                delta: expected_block_ts - curr_block_ts,
                direction: TimestampAdjustmentDirection::Backward,
            }))
        }
    }

    pub fn update_validators<SCT>(
        &mut self,
        validators: &[ValidatorData<SCT>],
        my_node: &NodeId<P>,
        epoch: &Epoch,
    ) where
        SCT: SignatureCollection<NodeIdPubKey = P>,
    {
        self.ping_state
            .update_validators(validators, my_node, epoch);
    }

    pub fn tick(&mut self) -> Vec<(NodeId<P>, PingSequence)> {
        self.ping_state.tick()
    }

    pub fn pong_received(&mut self, node_id: NodeId<P>, sequence: PingSequence) {
        self.ping_state.pong_received(node_id, sequence);
    }

    pub fn vote_sent(&mut self, round: Round) {
        self.last_sent_vote = Some(SentVote {
            round,
            timestamp: Instant::now(),
        });
    }

    pub fn enter_round(&mut self, epoch: &Epoch) {
        assert!(*epoch >= self.ping_state.current_epoch);
        debug!(?epoch, "Enter epoch");
        if *epoch > self.ping_state.current_epoch {
            self.ping_state.current_epoch = *epoch;
            self.ping_state
                .epoch_validators
                .retain(|key, _| key >= epoch);
            self.ping_state.compute_schedule();
        }
    }

    pub fn update<SCT>(
        &mut self,
        event: BlockTimestampEvent<SCT>,
    ) -> Vec<BlockTimestampCommand<SCT>>
    where
        SCT: SignatureCollection<NodeIdPubKey = P>,
    {
        match event {
            BlockTimestampEvent::PingRequest { sender, sequence } => {
                let mut cmds = Vec::new();
                tracing::debug!(?sender, ?sequence, "received ping request");
                cmds.push(BlockTimestampCommand::<SCT>::SendPong { target: sender, sequence});
                cmds
            }
            BlockTimestampEvent::PingResponse { sender, sequence } => {
                tracing::debug!(?sender, ?sequence, "received ping response");
                self.pong_received(sender, sequence);
                vec![]
            }
            BlockTimestampEvent::PingTick{} => {
                tracing::debug!("ping tick");
                self
                    .tick()
                    .into_iter()
                    .map(|(node, sequence)| {
                        tracing::debug!(?node, ?sequence, "sending ping request");
                        BlockTimestampCommand::<SCT>::SendPing { target: node, sequence}
                    })
                    .collect::<Vec<BlockTimestampCommand::<SCT>>>()
            }
            BlockTimestampEvent::TimestampEnterRound { round, epoch } => {
                self.enter_round(&epoch);
                vec![]
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::BTreeSet, time::Duration};

    use monad_consensus_types::quorum_certificate::{
        TimestampAdjustment, TimestampAdjustmentDirection,
    };
    use monad_crypto::{
        certificate_signature::CertificateKeyPair, NopKeyPair, NopPubKey, NopSignature,
    };
    use monad_testutil::signing::{create_keys, get_key, MockSignatures};
    use monad_types::{Epoch, NodeId, Round, Stake};

    use super::{Error, PING_PERIOD_SEC};
    use crate::{
        timestamp::{PingState, ValidatorPingState},
        BlockTimestamp, ValidatorData,
    };

    type SignatureType = NopSignature;
    type SignatureCollection = MockSignatures<SignatureType>;

    #[test]
    fn test_block_timestamp_validate() {
        let mut b = BlockTimestamp::<NopPubKey>::new(10, 1);
        let author = NodeId::new(NopKeyPair::from_bytes(&mut [0; 32]).unwrap().pubkey());

        assert!(matches!(
            b.valid_block_timestamp(1, 1, 0, &author).err().unwrap(),
            Error::Invalid
        ));
        assert!(matches!(
            b.valid_block_timestamp(2, 1, 0, &author).err().unwrap(),
            Error::Invalid
        ));
        assert!(matches!(
            b.valid_block_timestamp(0, 11, 0, &author).err().unwrap(),
            Error::OutOfBounds
        ));

        b.ping_state
            .validators
            .insert(author, ValidatorPingState::new());

        b.ping_state
            .validators
            .get_mut(&author)
            .unwrap()
            .update_latency(Duration::from_nanos(1));

        b.update_time(10);
        b.vote_sent(Round(0));

        assert!(matches!(
            b.valid_block_timestamp(11, 11, 0, &author).err().unwrap(),
            Error::Invalid
        ));
        assert!(matches!(
            b.valid_block_timestamp(12, 11, 0, &author).err().unwrap(),
            Error::Invalid
        ));
        assert!(matches!(
            b.valid_block_timestamp(9, 21, 0, &author).err().unwrap(),
            Error::OutOfBounds
        ));

        b.update_time(12);

        assert!(matches!(
            b.valid_block_timestamp(11, 20, 0, &author),
            Ok(Some(TimestampAdjustment {
                delta: 9,
                direction: TimestampAdjustmentDirection::Forward
            }))
        ));

        assert!(matches!(
            b.valid_block_timestamp(9, 12, 0, &author),
            Ok(Some(TimestampAdjustment {
                delta: 1,
                direction: TimestampAdjustmentDirection::Forward
            }))
        ));

        assert!(matches!(
            b.valid_block_timestamp(5, 10, 0, &author),
            Ok(Some(TimestampAdjustment {
                delta: 1,
                direction: TimestampAdjustmentDirection::Backward
            }))
        ));
    }

    #[test]
    fn test_ping_state() {
        let mut state = ValidatorPingState::new();
        let seq = state.start_next();
        state.update_latency(Duration::from_millis(1));

        assert_eq!(state.avg_latency().unwrap().as_millis(), 1);
        state.update_latency(Duration::from_millis(3));
        assert_eq!(state.avg_latency().unwrap().as_millis(), 2);
        state.update_latency(Duration::from_millis(6));
        assert_eq!(state.avg_latency().unwrap().as_millis(), 3);

        for _ in 0..100 {
            state.update_latency(Duration::from_millis(10));
        }

        assert_eq!(
            state.ping_latency.sum,
            state.ping_latency.samples.iter().sum::<Duration>()
        );

        assert_eq!(
            state.avg_latency().unwrap(),
            state.ping_latency.sum / state.ping_latency.samples.len() as u32
        );

        for _ in 0..50 {
            state.update_latency(Duration::from_millis(50));
        }

        assert_eq!(
            state.ping_latency.sum,
            state.ping_latency.samples.iter().sum::<Duration>()
        );

        assert_eq!(
            state.avg_latency().unwrap(),
            state.ping_latency.sum / state.ping_latency.samples.len() as u32
        );
    }

    #[test]
    fn test_update_validators() {
        let mut s = PingState::<NopPubKey>::new();

        let keys = create_keys::<NopSignature>(3);
        let nodes: Vec<_> = keys.iter().map(|k| NodeId::new(k.pubkey())).collect();

        let my_node = nodes[0];

        let validators = vec![
            ValidatorData::<SignatureCollection> {
                node_id: nodes[1],
                stake: Stake(1),
                cert_pubkey: nodes[1].pubkey(),
            },
            ValidatorData {
                node_id: nodes[2],
                stake: Stake(1),
                cert_pubkey: nodes[2].pubkey(),
            },
        ];

        s.update_validators(&validators, &my_node, &Epoch(1));
        assert_eq!(s.epoch_validators.len(), 1);
        assert_eq!(s.epoch_validators.get(&Epoch(1)).unwrap().len(), 2);

        // test sending identical to existing set of validators for the same epoch
        s.update_validators(&validators, &my_node, &Epoch(1));
        assert_eq!(s.epoch_validators.len(), 1);
        assert_eq!(s.epoch_validators.get(&Epoch(1)).unwrap().len(), 2);

        let validators_1 = vec![validators[0].clone()];

        s.update_validators(&validators_1, &my_node, &Epoch(2));
        assert_eq!(s.epoch_validators.get(&Epoch(2)).unwrap().len(), 1);

        assert!(!s.validators.contains_key(&nodes[1]));

        s.compute_schedule();

        assert!(s.validators.contains_key(&nodes[1]));

        let validators_2 = vec![validators[1].clone()];
        s.update_validators(&validators_2, &my_node, &Epoch(3));

        assert_eq!(s.epoch_validators.get_mut(&Epoch(3)).unwrap().len(), 1);
        s.compute_schedule();
        assert!(s.validators.contains_key(&nodes[2]));
    }

    #[test]
    fn test_enter_round() {
        let mut b = BlockTimestamp::<NopPubKey>::new(10, 1);

        let val_cnt = 5;
        let keys = create_keys::<NopSignature>(val_cnt);
        let nodes = keys
            .iter()
            .map(|k| NodeId::new(k.pubkey()))
            .collect::<Vec<_>>();

        let my_node = nodes[0];

        let e1 = Epoch(1);
        let e1_vals = vec![
            ValidatorData {
                node_id: nodes[0],
                stake: Stake(1),
                cert_pubkey: nodes[0].pubkey(),
            },
            ValidatorData::<SignatureCollection> {
                node_id: nodes[1],
                stake: Stake(1),
                cert_pubkey: nodes[1].pubkey(),
            },
            ValidatorData {
                node_id: nodes[2],
                stake: Stake(1),
                cert_pubkey: nodes[2].pubkey(),
            },
        ];

        let e2 = Epoch(2);
        let e2_vals = vec![
            ValidatorData::<SignatureCollection> {
                node_id: nodes[2],
                stake: Stake(1),
                cert_pubkey: nodes[2].pubkey(),
            },
            ValidatorData {
                node_id: nodes[3],
                stake: Stake(1),
                cert_pubkey: nodes[3].pubkey(),
            },
        ];

        let mut pings: Vec<NodeId<NopPubKey>> = Vec::new();

        let expected_e1 = [e1_vals[1].node_id, e1_vals[2].node_id];
        b.update_validators(&e1_vals, &my_node, &e1);
        let vals = b
            .ping_state
            .epoch_validators
            .get(&e1)
            .expect("get validators");
        assert!(vals.iter().all(|x| expected_e1.contains(x)));

        b.enter_round(&e1);
        assert_eq!(b.ping_state.current_epoch, e1);
        assert!(b
            .ping_state
            .validators
            .keys()
            .all(|x| expected_e1.contains(x)));
        assert_eq!(b.ping_state.validators.keys().len(), expected_e1.len());
        pings.clear();

        for _ in 0..PING_PERIOD_SEC {
            pings.extend(
                b.tick()
                    .iter()
                    .map(|x| x.0)
                    .collect::<Vec<NodeId<NopPubKey>>>(),
            );
        }
        assert!(expected_e1.iter().all(|x| pings.contains(x)));
        assert_eq!(expected_e1.len(), pings.len());

        let expected_e2 = [e2_vals[0].node_id, e2_vals[1].node_id];
        b.update_validators(&e2_vals, &my_node, &e2);
        b.enter_round(&e2);
        assert_eq!(b.ping_state.current_epoch, e2);
        assert!(b
            .ping_state
            .validators
            .keys()
            .all(|x| expected_e2.contains(x)));
        assert_eq!(b.ping_state.validators.keys().len(), expected_e2.len());
        pings.clear();

        for _ in 0..PING_PERIOD_SEC {
            pings.extend(
                b.tick()
                    .iter()
                    .map(|x| x.0)
                    .collect::<Vec<NodeId<NopPubKey>>>(),
            );
        }
        assert!(expected_e2.iter().all(|x| pings.contains(x)));
        assert_eq!(pings.len(), expected_e2.len());

        // Test that BlockTimestamp works as expected after entering the same epoch again.
        b.enter_round(&e2);
        assert_eq!(b.ping_state.current_epoch, e2);
        assert!(b
            .ping_state
            .validators
            .keys()
            .all(|x| expected_e2.contains(x)));
        assert_eq!(b.ping_state.validators.keys().len(), expected_e2.len());
        pings.clear();

        for _ in 0..PING_PERIOD_SEC {
            pings.extend(
                b.tick()
                    .iter()
                    .map(|x| x.0)
                    .collect::<Vec<NodeId<NopPubKey>>>(),
            );
        }
        assert!(expected_e2.iter().all(|x| pings.contains(x)));
        assert_eq!(pings.len(), expected_e2.len());
    }

    #[test]
    fn test_new_epoch() {
        let mut b = BlockTimestamp::<NopPubKey>::new(10, 1);

        let val_cnt = 5;
        let keys = create_keys::<NopSignature>(val_cnt);
        let nodes = keys
            .iter()
            .map(|k| NodeId::new(k.pubkey()))
            .collect::<Vec<_>>();

        let my_node = nodes[0];

        let e1 = Epoch(1);
        let e1_vals = vec![
            ValidatorData {
                node_id: nodes[0],
                stake: Stake(1),
                cert_pubkey: nodes[0].pubkey(),
            },
            ValidatorData::<SignatureCollection> {
                node_id: nodes[1],
                stake: Stake(1),
                cert_pubkey: nodes[1].pubkey(),
            },
            ValidatorData {
                node_id: nodes[2],
                stake: Stake(1),
                cert_pubkey: nodes[2].pubkey(),
            },
        ];

        let e2 = Epoch(2);
        let e2_vals = vec![
            ValidatorData::<SignatureCollection> {
                node_id: nodes[2],
                stake: Stake(1),
                cert_pubkey: nodes[2].pubkey(),
            },
            ValidatorData {
                node_id: nodes[3],
                stake: Stake(1),
                cert_pubkey: nodes[3].pubkey(),
            },
        ];

        let e3 = Epoch(3);
        let e3_vals = vec![
            ValidatorData::<SignatureCollection> {
                node_id: nodes[3],
                stake: Stake(1),
                cert_pubkey: nodes[3].pubkey(),
            },
            ValidatorData {
                node_id: nodes[4],
                stake: Stake(1),
                cert_pubkey: nodes[4].pubkey(),
            },
        ];

        let expected_e1 = [e1_vals[1].node_id, e1_vals[2].node_id];
        let expected_e2 = [e2_vals[0].node_id, e2_vals[1].node_id];

        let mut pings: Vec<NodeId<NopPubKey>> = Vec::new();

        b.update_validators(&e1_vals, &my_node, &e1);
        b.update_validators(&e2_vals, &my_node, &e2);
        b.update_validators(&e3_vals, &my_node, &e3);
        assert_eq!(b.ping_state.epoch_validators.len(), 3);
        let vals = b
            .ping_state
            .epoch_validators
            .get(&e1)
            .expect("get validators");
        assert_eq!(
            vals.iter().copied().collect::<BTreeSet<_>>(),
            expected_e1.iter().copied().collect::<BTreeSet<_>>()
        );

        let vals = b
            .ping_state
            .epoch_validators
            .get(&e2)
            .expect("get validators");
        assert_eq!(
            vals.iter().copied().collect::<BTreeSet<_>>(),
            expected_e2.iter().copied().collect::<BTreeSet<_>>()
        );

        let expected_e1_e2 = [e1_vals[1].node_id, e2_vals[0].node_id, e2_vals[1].node_id];
        b.enter_round(&e1);

        assert_eq!(
            b.ping_state
                .validators
                .keys()
                .copied()
                .collect::<BTreeSet<_>>(),
            expected_e1_e2.iter().copied().collect::<BTreeSet<_>>()
        );
        pings.clear();

        for _ in 0..PING_PERIOD_SEC {
            pings.extend(
                b.tick()
                    .iter()
                    .map(|x| x.0)
                    .collect::<Vec<NodeId<NopPubKey>>>(),
            );
        }
        assert!(expected_e1_e2.iter().all(|x| pings.contains(x)));
        assert_eq!(pings.len(), expected_e1_e2.len());

        let expected_e2_e3 = [e2_vals[0].node_id, e2_vals[1].node_id, e3_vals[1].node_id];
        b.enter_round(&e2);
        assert_eq!(
            b.ping_state
                .validators
                .keys()
                .copied()
                .collect::<BTreeSet<_>>(),
            expected_e2_e3.iter().copied().collect::<BTreeSet<_>>()
        );
        pings.clear();

        for _ in 0..PING_PERIOD_SEC {
            pings.extend(
                b.tick()
                    .iter()
                    .map(|x| x.0)
                    .collect::<Vec<NodeId<NopPubKey>>>(),
            );
        }

        assert_eq!(
            pings.iter().copied().collect::<BTreeSet<_>>(),
            expected_e2_e3.iter().copied().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn test_ticks() {
        let mut s = PingState::<NopPubKey>::new();

        //let my_key = NopKeyPair::from_bytes(&mut [0; 32]).unwrap().pubkey();
        let my_key = get_key::<SignatureType>(1_u64).pubkey();
        let my_node = NodeId::new(my_key);

        let k_1 = get_key::<SignatureType>(2_u64).pubkey();
        let node_1 = NodeId::new(k_1);

        let k_2 = get_key::<SignatureType>(3_u64).pubkey();
        let node_2 = NodeId::new(k_2);

        let validators = vec![
            ValidatorData::<SignatureCollection> {
                node_id: node_1,
                stake: Stake(1),
                cert_pubkey: node_1.pubkey(),
            },
            ValidatorData {
                node_id: node_2,
                stake: Stake(1),
                cert_pubkey: node_2.pubkey(),
            },
        ];

        s.current_epoch = Epoch(1);
        s.update_validators(&validators, &my_node, &Epoch(1));
        s.compute_schedule();

        let mut pings = Vec::new();
        for _ in 0..PING_PERIOD_SEC {
            pings.extend(
                s.tick()
                    .iter()
                    .map(|x| x.0)
                    .collect::<Vec<NodeId<NopPubKey>>>(),
            );
        }

        assert_eq!(
            pings.iter().copied().collect::<BTreeSet<_>>(),
            validators
                .iter()
                .map(|x| x.node_id)
                .collect::<BTreeSet<_>>()
        );
    }
}
