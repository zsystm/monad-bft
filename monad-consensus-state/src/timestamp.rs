use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    time::{Duration, Instant, SystemTime},
};

use monad_consensus_types::{
    quorum_certificate::{TimestampAdjustment, TimestampAdjustmentDirection},
    signature_collection::SignatureCollection,
    validator_data::ValidatorData,
};
use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, PingSequence, Round};
use tracing::info;

const MAX_LATENCY_SAMPLES: usize = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Invalid,     // timestamp did not increment compared to previous block
    OutOfBounds, // timestamp is out of bounds compared to local time
}

#[derive(Debug)]
struct RunningAverage {
    sum: Duration,
    max_samples: usize,
    samples: Vec<Duration>,
    avg: Option<Duration>,
    next_index: usize,
}

impl RunningAverage {
    pub fn new(max_samples: usize) -> Self {
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
#[derive(Debug)]
pub struct ValidatorPingState {
    sequence: PingSequence,       // sequence number of the last ping sent
    ping_latency: RunningAverage, // average latency of the last pings
    last_ping_time: Instant,      // time of the last ping sent
    proposal_ping_time: Duration,
    proposal_ping_round: Round,
    proposal_received: Duration,
    proposal_received_round: Round,
    proposal_latency: RunningAverage,
}

impl Default for ValidatorPingState {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            sequence: PingSequence(0),
            ping_latency: RunningAverage::new(MAX_LATENCY_SAMPLES),
            last_ping_time: now,
            proposal_ping_time: Default::default(),
            proposal_ping_round: Round(0),
            proposal_received: Default::default(),
            proposal_received_round: Round(0),
            proposal_latency: RunningAverage::new(MAX_LATENCY_SAMPLES),
        }
    }
}

impl ValidatorPingState {
    pub fn new() -> Self {
        Self::default()
    }

    // initiate a new ping
    pub fn start_next(&mut self) -> PingSequence {
        self.sequence.0 += 1;
        self.last_ping_time = Instant::now();
        self.sequence
    }

    fn update_latency(&mut self, latency: Duration) {
        self.ping_latency.add_sample(latency);
    }

    pub fn pong_received(&mut self, sequence: PingSequence) {
        if sequence != self.sequence {
            return;
        }
        // estimate latency as half the round trip time
        self.update_latency(self.last_ping_time.elapsed() / 2);
        info!("ping latency {:?}", self.ping_latency.get_avg());
    }

    pub fn avg_latency(&self) -> Option<Duration> {
        self.ping_latency.get_avg()
    }

    // approximate the time the broadcasted proposal was sent from the time direct ping
    // was received and the average ping latency
    fn add_proposal_latency(&mut self) {
        self.proposal_latency.add_sample(
            self.proposal_received.saturating_sub(
                self.proposal_ping_time
                    .saturating_sub(self.avg_latency().unwrap_or_default()),
            ),
        );
        info!(
            "proposal latency {:?} avg {:?}",
            self.proposal_received.saturating_sub(
                self.proposal_ping_time
                    .saturating_sub(self.avg_latency().unwrap_or_default())
            ),
            self.proposal_latency.get_avg().unwrap_or_default()
        );
    }

    fn proposal_ping_received(&mut self, round: Round, recv_timestamp: Duration) {
        if round <= self.proposal_ping_round {
            return;
        }
        self.proposal_ping_time = recv_timestamp;
        self.proposal_ping_round = round;
        if self.proposal_received_round == round {
            self.add_proposal_latency();
        }
    }

    fn proposal_received(&mut self, round: Round, recv_timestamp: Duration) {
        if round <= self.proposal_received_round {
            return;
        }
        self.proposal_received = recv_timestamp;
        self.proposal_received_round = round;
        if self.proposal_ping_round == round {
            self.add_proposal_latency();
        }
    }
}

const PING_PERIOD: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct PingState<P: PubKey> {
    validators: HashMap<NodeId<P>, ValidatorPingState>, // ping state per validator
    schedule: Vec<Vec<NodeId<P>>>, // schedule of validators to ping, length is period
    tick: usize,                   // current tick index into schedule
    period: usize,                 // number of ticks in full schedule
    schedule_time: Instant,
    start_time: Instant,
}

fn duration_ceil(duration: Duration) -> Duration {
    let secs = duration.as_secs();
    let nanos = duration.subsec_nanos();
    if nanos > 0 {
        Duration::new(secs + 1, 0)
    } else {
        Duration::new(secs, 0)
    }
}

impl<P: PubKey> PingState<P> {
    pub fn new() -> Self {
        let period = PING_PERIOD.as_secs() as usize;
        let now = Instant::now();
        Self {
            validators: HashMap::new(),
            schedule: vec![Vec::new(); period],
            tick: 0,
            period,
            start_time: now,
            schedule_time: now,
        }
    }

    fn next_validator_set_at(&mut self, instant: Instant) -> Vec<NodeId<P>> {
        if instant <= self.schedule_time {
            return Vec::new();
        }
        let index = (self.schedule_time - self.start_time).as_secs() % self.period as u64;
        let secs_passed = duration_ceil(instant - self.schedule_time).as_secs();
        let num_expired = std::cmp::min(secs_passed, self.period as u64);
        self.schedule_time = self.schedule_time + Duration::from_secs(secs_passed);
        (index..index + num_expired)
            .flat_map(|i| self.schedule[i as usize % self.period].iter())
            .cloned()
            .collect()
    }

    pub fn next_validator_set(&mut self) -> Vec<NodeId<P>> {
        self.next_validator_set_at(Instant::now())
    }

    pub fn pong_received(&mut self, node_id: NodeId<P>, sequence: PingSequence) {
        if let Some(validator_state) = self.validators.get_mut(&node_id) {
            validator_state.pong_received(sequence);
            info!(
                "node {:?} latency {:?}",
                node_id,
                validator_state.avg_latency()
            );
        }
    }

    fn update_validators<SCT>(&mut self, validators: &Vec<ValidatorData<SCT>>, my_node: &NodeId<P>)
    where
        SCT: SignatureCollection<NodeIdPubKey = P>,
    {
        let removed_nodes = self
            .validators
            .keys()
            .filter(|node_id| !validators.iter().any(|v| v.node_id == **node_id))
            .cloned()
            .collect::<Vec<_>>();
        for node_id in removed_nodes {
            self.validators.remove(&node_id);
        }
        for validator in validators {
            if validator.node_id == *my_node {
                continue;
            }
            self.validators
                .entry(NodeId::new(validator.node_id.pubkey()))
                .or_default();
        }

        // map validators to schedule slots by hashing node id
        for s in self.schedule.iter_mut() {
            s.clear();
        }
        for node in self.validators.keys() {
            let mut hasher = DefaultHasher::new();
            node.hash(&mut hasher);
            let idx = (hasher.finish() as usize) % self.period;
            self.schedule[idx].push(*node);
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
        self.tick = (self.tick + 1) % self.period;
        pings
    }

    fn get_latency(&self, node_id: &NodeId<P>) -> Option<Duration> {
        self.validators.get(node_id).and_then(|v| v.avg_latency())
    }

    fn get_proposal_latency(&self, node_id: &NodeId<P>) -> Option<Duration> {
        self.validators
            .get(node_id)
            .and_then(|v| v.proposal_latency.get_avg())
    }

    fn proposal_ping_received(
        &mut self,
        round: Round,
        author: &NodeId<P>,
        recv_timestamp: Duration,
    ) {
        if let Some(validator_state) = self.validators.get_mut(author) {
            validator_state.proposal_ping_received(round, recv_timestamp);
        }
    }

    fn proposal_received(&mut self, round: Round, author: &NodeId<P>, recv_timestamp: Duration) {
        if let Some(validator_state) = self.validators.get_mut(author) {
            validator_state.proposal_received(round, recv_timestamp);
        }
    }
}

#[derive(Debug)]
struct SentVote {
    round: Round,
    timestamp: Instant,
}

#[derive(Debug)]
pub struct BlockTimestamp<P: PubKey> {
    local_time: u64,

    max_delta: u64,

    ping_state: PingState<P>,

    sent_vote: Option<SentVote>, // last voted round and timestamp

    /// TODO: this needs an upper-bound
    latency_estimate_ms: u64,
}

impl<P: PubKey> BlockTimestamp<P> {
    pub fn new(max_delta: u64, latency_estimate_ms: u64) -> Self {
        assert!(latency_estimate_ms > 0);
        Self {
            local_time: 0,
            max_delta,
            latency_estimate_ms,
            ping_state: PingState::new(),
            sent_vote: None,
        }
    }

    pub fn update_time(&mut self, time: u64) {
        self.local_time = time;
    }

    pub fn get_current_time(&self) -> u64 {
        self.local_time
    }

    pub fn get_valid_block_timestamp(&self, prev_block_ts: u64) -> u64 {
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        if time <= prev_block_ts {
            prev_block_ts + 1
        } else {
            time
        }
    }

    fn valid_bounds(&self, timestamp: u64) -> bool {
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let lower_bound = time.saturating_sub(self.max_delta);
        let upper_bound = time.saturating_add(self.max_delta);

        lower_bound <= timestamp && timestamp <= upper_bound
    }

    pub fn valid_block_timestamp(
        &self,
        recv_timestamp: Duration,
        prev_block_ts: u64,
        curr_block_ts: u64,
        round: Round,
        author: &NodeId<P>,
    ) -> Result<Option<TimestampAdjustment>, Error> {
        if curr_block_ts <= prev_block_ts {
            // block timestamp must be strictly monotonically increasing
            return Err(Error::Invalid);
        }
        if !self.valid_bounds(curr_block_ts) {
            return Err(Error::OutOfBounds);
        }
        // return the delta between expected block time and actual block time for adjustment
        match &self.sent_vote {
            Some(vote) if vote.round + Round(1) == round => {
                let latency = self
                    .ping_state
                    .get_proposal_latency(author)
                    .unwrap_or(Duration::from_millis(self.latency_estimate_ms));
                let time = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                let expected_block_ts =
                    (recv_timestamp.as_millis() as u64).saturating_sub(latency.as_millis() as u64);
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
            _ => Ok(None),
        }
    }

    pub fn update_validators<SCT>(
        &mut self,
        validators: &Vec<ValidatorData<SCT>>,
        my_node: &NodeId<P>,
    ) where
        SCT: SignatureCollection<NodeIdPubKey = P>,
    {
        self.ping_state.update_validators(validators, my_node);
    }

    pub fn tick(&mut self) -> Vec<(NodeId<P>, PingSequence)> {
        self.ping_state.tick()
    }

    pub fn pong_received(&mut self, node_id: NodeId<P>, sequence: PingSequence) {
        self.ping_state.pong_received(node_id, sequence);
    }

    pub fn vote_sent(&mut self, round: Round) {
        info!("vote sent for round {:?}", round);
        match &self.sent_vote {
            Some(vote) if vote.round >= round => {
                info!("vote already sent for round {:?}", round);
            }
            _ => {
                self.sent_vote = Some(SentVote {
                    round,
                    timestamp: Instant::now(),
                });
            }
        }
    }

    pub fn next_validator_set(&mut self) -> Vec<NodeId<P>> {
        self.ping_state.next_validator_set()
    }

    pub fn proposal_ping_received(
        &mut self,
        round: Round,
        author: &NodeId<P>,
        recv_timestamp: Duration,
    ) {
        info!(
            "proposal ping received for round {:?} author {:?}",
            round, author
        );
        self.ping_state
            .proposal_ping_received(round, author, recv_timestamp);
    }

    pub fn proposal_received(
        &mut self,
        round: Round,
        author: &NodeId<P>,
        recv_timestamp: Duration,
    ) {
        info!(
            "proposal received for round {:?} author {:?}",
            round, author
        );
        self.ping_state
            .proposal_received(round, author, recv_timestamp);
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use monad_consensus_types::quorum_certificate::{
        TimestampAdjustment, TimestampAdjustmentDirection,
    };
    use monad_crypto::{
        certificate_signature::CertificateKeyPair, NopKeyPair, NopPubKey, NopSignature,
    };
    use monad_types::{NodeId, Round, Stake};

    use crate::{
        timestamp::{PingState, ValidatorPingState},
        BlockTimestamp, ValidatorData,
    };

    #[test]
    fn test_block_timestamp_validate() {
        let mut b = BlockTimestamp::<NopPubKey>::new(10, 1);
        let author = NodeId::new(NopKeyPair::from_bytes(&mut [0; 32]).unwrap().pubkey());

        b.ping_state
            .validators
            .insert(author, ValidatorPingState::new());
        b.ping_state
            .validators
            .get_mut(&author)
            .unwrap()
            .update_latency(Duration::from_millis(1));

        b.update_time(10);
        b.vote_sent(Round(0));

        assert!(b
            .valid_block_timestamp(Default::default(), 11, 11, Round(1), &author)
            .is_err());
        assert!(b
            .valid_block_timestamp(Default::default(), 12, 11, Round(1), &author)
            .is_err());
        assert!(b
            .valid_block_timestamp(Default::default(), 9, 21, Round(1), &author)
            .is_err());

        b.update_time(12);
        std::thread::sleep(Duration::from_millis(2));

        assert!(matches!(
            b.valid_block_timestamp(Default::default(), 9, 12, Round(1), &author),
            Ok(Some(TimestampAdjustment {
                delta: 1,
                direction: TimestampAdjustmentDirection::Forward
            }))
        ));

        assert!(matches!(
            b.valid_block_timestamp(Default::default(), 5, 10, Round(1), &author),
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

    use monad_testutil::signing::MockSignatures;

    use super::PING_PERIOD;

    type SignatureType = NopSignature;
    type SignatureCollection = MockSignatures<SignatureType>;

    #[test]
    fn test_update_validators() {
        let mut s = PingState::<NopPubKey>::new();

        let my_key = NopKeyPair::from_bytes(&mut [0; 32]).unwrap().pubkey();
        let my_node = NodeId::new(my_key);

        let k_1 = NopKeyPair::from_bytes(&mut [1; 32]).unwrap().pubkey();
        let node_1 = NodeId::new(k_1);

        let k_2 = NopKeyPair::from_bytes(&mut [2; 32]).unwrap().pubkey();
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

        s.update_validators(&validators, &my_node);

        assert_eq!(s.validators.len(), 2);

        let validators_1 = vec![validators[0].clone()];

        s.update_validators(&validators_1, &my_node);

        assert_eq!(s.validators.len(), 1);
        assert!(s.validators.contains_key(&node_1));

        let validators_2 = vec![validators[1].clone()];
        s.update_validators(&validators_2, &my_node);

        assert_eq!(s.validators.len(), 1);
        assert!(s.validators.contains_key(&node_2));
    }

    #[test]
    fn test_ticks() {
        let mut s = PingState::<NopPubKey>::new();

        let my_key = NopKeyPair::from_bytes(&mut [0; 32]).unwrap().pubkey();
        let my_node = NodeId::new(my_key);

        let k_1 = NopKeyPair::from_bytes(&mut [1; 32]).unwrap().pubkey();
        let node_1 = NodeId::new(k_1);

        let k_2 = NopKeyPair::from_bytes(&mut [2; 32]).unwrap().pubkey();
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

        s.update_validators(&validators, &my_node);

        let mut pings = Vec::new();
        for _ in 0..PING_PERIOD.as_secs() {
            pings.extend(s.tick());
        }

        assert_eq!(
            pings.len(),
            validators.len(),
            "number of pings in total period should match number of validators"
        );
    }

    #[test]
    fn test_next_validator_set() {
        let mut s = PingState::<NopPubKey>::new();

        let t = s.schedule_time;
        let vs = s.next_validator_set_at(t + Duration::from_millis(1));
        assert_eq!(vs.len(), 0);
        assert_eq!(s.schedule_time, t + Duration::from_secs(1));

        for i in 0..PING_PERIOD.as_secs() {
            s.schedule[i as usize].push(NodeId::new(
                NopKeyPair::from_bytes(&mut [i as u8; 32]).unwrap().pubkey(),
            ));
        }

        let t = s.schedule_time;
        let vs = s.next_validator_set_at(t + Duration::from_millis(1));
        assert_eq!(vs.len(), 1);
        assert_eq!(vs[0].pubkey(), s.schedule[1][0].pubkey());
        assert_eq!(s.schedule_time, t + Duration::from_secs(1));

        let t = s.schedule_time;
        let vs = s.next_validator_set_at(t + Duration::from_secs(1));
        assert_eq!(vs.len(), 1);
        assert_eq!(vs[0].pubkey(), s.schedule[2][0].pubkey());
        assert_eq!(s.schedule_time, t + Duration::from_secs(1));

        let t = s.schedule_time;
        let vs = s.next_validator_set_at(t + Duration::from_secs(1) + Duration::from_millis(1));
        assert_eq!(vs.len(), 2);
        assert_eq!(vs[0].pubkey(), s.schedule[3][0].pubkey());
        assert_eq!(vs[1].pubkey(), s.schedule[4][0].pubkey());
        assert_eq!(s.schedule_time, t + Duration::from_secs(2));

        let t = s.schedule_time;
        let vs = s.next_validator_set_at(t + Duration::from_secs(50) + Duration::from_millis(1));
        assert_eq!(vs.len(), PING_PERIOD.as_secs() as usize);
        assert_eq!(s.schedule_time, t + Duration::from_secs(51));
    }
}
