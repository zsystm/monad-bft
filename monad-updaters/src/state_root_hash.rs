use std::{
    collections::VecDeque,
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::Stream;
use monad_consensus_types::{
    signature_collection::SignatureCollection,
    state_root_hash::{StateRootHash, StateRootHashInfo},
    validator_data::ValidatorSetData,
};
use monad_crypto::{certificate_signature::CertificateSignatureRecoverable, hasher::Hash};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, ProposedStateRoot, StateRootEvent, StateRootHashCommand};
use monad_types::{Epoch, SeqNum, Stake};
use rand::RngCore;
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
use reth_primitives::Header;
use tracing::{debug, error};

pub trait MockableStateRootHash:
    Executor<Command = StateRootHashCommand<Self::SignatureCollection>>
    + Stream<Item = Self::Event>
    + Unpin
{
    type Event;
    type SignatureCollection: SignatureCollection;

    fn ready(&self) -> bool;
    fn get_validator_set_data(&self, epoch: Epoch) -> ValidatorSetData<Self::SignatureCollection>;
    fn compute_execution_result(&self, seq_num: &SeqNum) -> Header;
    fn set_state_root_poll(&mut self, keep_update: bool);
}

impl<T: MockableStateRootHash + ?Sized> MockableStateRootHash for Box<T> {
    type Event = T::Event;
    type SignatureCollection = T::SignatureCollection;

    fn ready(&self) -> bool {
        (**self).ready()
    }

    fn get_validator_set_data(&self, epoch: Epoch) -> ValidatorSetData<Self::SignatureCollection> {
        (**self).get_validator_set_data(epoch)
    }

    fn compute_execution_result(&self, seq_num: &SeqNum) -> Header {
        (**self).compute_execution_result(seq_num)
    }

    fn set_state_root_poll(&mut self, keep_update: bool) {
        (**self).set_state_root_poll(keep_update)
    }
}

/// Validator set update prepared from staking contract/execution
pub(crate) struct ValidatorSetUpdate<SCT: SignatureCollection> {
    /// Epoch for which the validator set is prepared
    pub epoch: Epoch,
    pub validator_data: ValidatorSetData<SCT>,
}

/// An updater that immediately creates a StateRootHash update and
/// the ValidatorSetData for the next epoch when it receives a
/// ledger commit command.
/// Goal is to mimic the behaviour of execution receiving a commit
/// and generating the state root hash and updating the staking contract,
/// and sending it back to consensus.
pub struct MockStateRootHashNop<ST, SCT: SignatureCollection> {
    state_root_update: VecDeque<StateRootEvent>,

    // validator set updates
    genesis_validator_data: ValidatorSetData<SCT>,
    next_val_data: Option<ValidatorSetUpdate<SCT>>,
    val_set_update_interval: SeqNum,
    calc_state_root: fn(&SeqNum) -> StateRootHash,

    last_sent_epoch: Option<SeqNum>,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    phantom: PhantomData<ST>,
}

impl<ST, SCT: SignatureCollection> MockStateRootHashNop<ST, SCT> {
    /// Defines how an honest mock execution calculates state root hash
    fn state_root_honest(seq_num: &SeqNum) -> StateRootHash {
        let mut gen = ChaChaRng::seed_from_u64(seq_num.0);
        let mut hash = StateRootHash(Hash([0; 32]));
        gen.fill_bytes(&mut hash.0 .0);
        hash
    }

    pub fn new(
        genesis_validator_data: ValidatorSetData<SCT>,
        val_set_update_interval: SeqNum,
    ) -> Self {
        Self {
            state_root_update: Default::default(),
            genesis_validator_data,
            next_val_data: None,
            val_set_update_interval,
            calc_state_root: Self::state_root_honest,

            last_sent_epoch: None,

            waker: None,
            metrics: Default::default(),
            phantom: PhantomData,
        }
    }

    /// Change how state root hash is calculated
    pub fn inject_byzantine_srh(&mut self, calc_srh: fn(&SeqNum) -> StateRootHash) {
        self.calc_state_root = calc_srh;
    }

    fn jank_update_valset(&mut self, seq_num: SeqNum) {
        if seq_num.is_epoch_end(self.val_set_update_interval)
            && self.last_sent_epoch != Some(seq_num)
        {
            if self.next_val_data.is_some() {
                error!("Validator set data is not consumed");
            }
            let locked_epoch = seq_num.get_locked_epoch(self.val_set_update_interval);
            assert_eq!(
                locked_epoch,
                seq_num.to_epoch(self.val_set_update_interval) + Epoch(2)
            );
            self.next_val_data = Some(ValidatorSetUpdate {
                epoch: locked_epoch,
                validator_data: self.genesis_validator_data.clone(),
            });
            self.last_sent_epoch = Some(seq_num);
        }
    }
}

impl<ST, SCT> MockableStateRootHash for MockStateRootHashNop<ST, SCT>
where
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Event = MonadEvent<ST, SCT>;
    type SignatureCollection = SCT;

    fn ready(&self) -> bool {
        !self.state_root_update.is_empty() || self.next_val_data.is_some()
    }

    fn get_validator_set_data(&self, _epoch: Epoch) -> ValidatorSetData<Self::SignatureCollection> {
        self.genesis_validator_data.clone()
    }

    fn compute_execution_result(&self, seq_num: &SeqNum) -> Header {
        // hash is pseudorandom seeded by the block's seq num to ensure
        // that it is deterministic between nodes
        let state_root_hash = (self.calc_state_root)(seq_num);
        debug!(
            "block number {:?} state root hash {:?}",
            seq_num.0, state_root_hash
        );

        let mut header = Header::default();
        header.number = seq_num.0;
        header.state_root = state_root_hash.0 .0.into();

        header
    }

    fn set_state_root_poll(&mut self, keep_update: bool) {
    }
}

impl<ST, SCT> Executor for MockStateRootHashNop<ST, SCT>
where
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Command = StateRootHashCommand<SCT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::CancelBelow(cancel_below) => {
                    while self
                        .state_root_update
                        .front()
                        .is_some_and(|state_root| state_root.seq_num() < cancel_below)
                    {
                        self.state_root_update.pop_front().unwrap();
                    }
                }
                StateRootHashCommand::RequestProposed(block_id, seq_num, round) => {
                    self.state_root_update
                        .push_back(StateRootEvent::Proposed(ProposedStateRoot {
                            block_id,
                            seq_num,
                            round,
                            result: self.compute_execution_result(&seq_num),
                        }));
                    self.jank_update_valset(seq_num);
                    wake = true;
                }
                StateRootHashCommand::RequestFinalized(seq_num) => {
                    self.state_root_update.push_back(StateRootEvent::Finalized(
                        seq_num,
                        self.compute_execution_result(&seq_num),
                    ));
                    self.jank_update_valset(seq_num);
                    wake = true;
                }
                StateRootHashCommand::UpdateValidators(_) => {
                    wake = true;
                }
            }
        }
        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            };
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT> Stream for MockStateRootHashNop<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        let event = if let Some(event) = this.state_root_update.pop_front() {
            Poll::Ready(Some(MonadEvent::StateRootEvent(event)))
        } else if let Some(next_val_data) = this.next_val_data.take() {
            Poll::Ready(Some(MonadEvent::ValidatorEvent(
                monad_executor_glue::ValidatorEvent::<SCT>::UpdateValidators((
                    next_val_data.validator_data,
                    next_val_data.epoch,
                )),
            )))
        } else {
            Poll::Pending
        };

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if this.ready() {
            this.waker.take().unwrap().wake();
        }

        event
    }
}

/// An updater that works the same as MockStateRootHashNop but switches
/// between two sets of validators every epoch.
/// Goal is to mimic new validators joining and old validators leaving.
pub struct MockStateRootHashSwap<ST, SCT: SignatureCollection> {
    state_root_update: VecDeque<StateRootEvent>,

    // validator set updates
    epoch: Epoch,
    genesis_val_data: ValidatorSetData<SCT>,
    val_data_1: ValidatorSetData<SCT>,
    val_data_2: ValidatorSetData<SCT>,
    next_val_data: Option<ValidatorSetUpdate<SCT>>,
    val_set_update_interval: SeqNum,

    last_sent_epoch: Option<SeqNum>,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    phantom: PhantomData<ST>,
}

impl<ST, SCT: SignatureCollection> MockStateRootHashSwap<ST, SCT> {
    pub fn new(
        genesis_validator_data: ValidatorSetData<SCT>,
        val_set_update_interval: SeqNum,
    ) -> Self {
        let num_validators = genesis_validator_data.0.len();
        let mut val_data_1 = genesis_validator_data.0.clone();
        let mut val_data_2 = val_data_1.clone();

        for validator in val_data_1.iter_mut().take(num_validators / 2) {
            validator.stake = Stake(0);
        }
        for validator in val_data_2
            .iter_mut()
            .take(num_validators)
            .skip(num_validators / 2)
        {
            validator.stake = Stake(0);
        }

        Self {
            state_root_update: Default::default(),
            epoch: Epoch(1),
            genesis_val_data: genesis_validator_data,
            val_data_1: ValidatorSetData(val_data_1),
            val_data_2: ValidatorSetData(val_data_2),
            next_val_data: None,
            val_set_update_interval,

            last_sent_epoch: None,

            waker: None,
            metrics: Default::default(),
            phantom: PhantomData,
        }
    }

    fn jank_update_valset(&mut self, seq_num: SeqNum) {
        if seq_num.is_epoch_end(self.val_set_update_interval)
            && self.last_sent_epoch != Some(seq_num)
        {
            if self.next_val_data.is_some() {
                error!("Validator set data is not consumed");
            }
            let locked_epoch = seq_num.get_locked_epoch(self.val_set_update_interval);
            assert_eq!(
                locked_epoch,
                seq_num.to_epoch(self.val_set_update_interval) + Epoch(2)
            );
            self.next_val_data = if locked_epoch.0 % 2 == 0 {
                Some(ValidatorSetUpdate {
                    epoch: locked_epoch,
                    validator_data: self.val_data_1.clone(),
                })
            } else {
                Some(ValidatorSetUpdate {
                    epoch: locked_epoch,
                    validator_data: self.val_data_2.clone(),
                })
            };
        }
        self.last_sent_epoch = Some(seq_num);
    }
}

impl<ST, SCT> MockableStateRootHash for MockStateRootHashSwap<ST, SCT>
where
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Event = MonadEvent<ST, SCT>;
    type SignatureCollection = SCT;

    fn ready(&self) -> bool {
        !self.state_root_update.is_empty() || self.next_val_data.is_some()
    }

    fn get_validator_set_data(&self, epoch: Epoch) -> ValidatorSetData<Self::SignatureCollection> {
        assert!(
            epoch <= self.epoch,
            "requesting epoch higher than seen in ledger"
        );

        // genesis epoch
        if epoch == Epoch(1) {
            return self.genesis_val_data.clone();
        }
        // in exec implementation
        // at the end of Epoch(even), next validator set is val_data_1
        // odd epoch number <> val_data_1
        // even epoch number <> val_data_2
        if epoch.0 % 2 == 0 {
            self.val_data_2.clone()
        } else {
            self.val_data_1.clone()
        }
    }

    fn compute_execution_result(&self, seq_num: &SeqNum) -> Header {
        let mut gen = ChaChaRng::seed_from_u64(seq_num.0);
        let mut hash = [0; 32];
        gen.fill_bytes(&mut hash);

        let mut header = Header::default();
        header.number = seq_num.0;
        header.state_root = hash.into();

        header
    }

    fn set_state_root_poll(&mut self, keep_update: bool) {
    }
}

impl<ST, SCT> Executor for MockStateRootHashSwap<ST, SCT>
where
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Command = StateRootHashCommand<SCT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::CancelBelow(cancel_below) => {
                    while self
                        .state_root_update
                        .front()
                        .is_some_and(|state_root| state_root.seq_num() < cancel_below)
                    {
                        self.state_root_update.pop_front().unwrap();
                    }
                }
                StateRootHashCommand::RequestProposed(block_id, seq_num, round) => {
                    self.state_root_update
                        .push_back(StateRootEvent::Proposed(ProposedStateRoot {
                            block_id,
                            seq_num,
                            round,
                            result: self.compute_execution_result(&seq_num),
                        }));
                    self.jank_update_valset(seq_num);
                    wake = true;
                }
                StateRootHashCommand::RequestFinalized(seq_num) => {
                    self.state_root_update.push_back(StateRootEvent::Finalized(
                        seq_num,
                        self.compute_execution_result(&seq_num),
                    ));
                    self.jank_update_valset(seq_num);
                    wake = true;
                }
                StateRootHashCommand::UpdateValidators(_) => {
                    wake = true;
                }
            }
        }
        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            };
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT> Stream for MockStateRootHashSwap<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        let event = if let Some(event) = this.state_root_update.pop_front() {
            Poll::Ready(Some(MonadEvent::StateRootEvent(event)))
        } else if let Some(next_val_data) = this.next_val_data.take() {
            Poll::Ready(Some(MonadEvent::ValidatorEvent(
                monad_executor_glue::ValidatorEvent::<SCT>::UpdateValidators((
                    next_val_data.validator_data,
                    next_val_data.epoch,
                )),
            )))
        } else {
            Poll::Pending
        };

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if this.ready() {
            this.waker.take().unwrap().wake();
        }

        event
    }
}

/// An updater that immediately creates a StateRootHash update and
/// the ValidatorSetData for the next epoch when it receives a
/// ledger commit command.
/// Goal is to mimic the behaviour of execution receiving a commit
/// and generating the state root hash and updating the staking contract,
/// and sending it back to consensus.
pub struct MockStateRootHashScheduler<ST, SCT: SignatureCollection> {
    state_root_update: VecDeque<(Duration, StateRootEvent)>,

    // validator set updates
    genesis_validator_data: ValidatorSetData<SCT>,
    next_val_data: Option<ValidatorSetUpdate<SCT>>,
    val_set_update_interval: SeqNum,
    calc_state_root: fn(&SeqNum) -> StateRootHash,

    last_sent_epoch: Option<SeqNum>,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    phantom: PhantomData<ST>,
    keep_update: bool,
}

impl<ST, SCT: SignatureCollection> MockStateRootHashScheduler<ST, SCT> {
    /// Defines how an honest mock execution calculates state root hash
    fn state_root_honest(seq_num: &SeqNum) -> StateRootHash {
        let mut gen = ChaChaRng::seed_from_u64(seq_num.0);
        let mut hash = StateRootHash(Hash([0; 32]));
        gen.fill_bytes(&mut hash.0 .0);
        hash
    }

    pub fn new(
        genesis_validator_data: ValidatorSetData<SCT>,
        val_set_update_interval: SeqNum,
    ) -> Self {
        Self {
            state_root_update: Default::default(),
            genesis_validator_data,
            next_val_data: None,
            val_set_update_interval,
            calc_state_root: Self::state_root_honest,

            last_sent_epoch: None,

            waker: None,
            metrics: Default::default(),
            phantom: PhantomData,
            keep_update: true,
        }
    }

    fn jank_update_valset(&mut self, seq_num: SeqNum) {
        if seq_num.is_epoch_end(self.val_set_update_interval)
            && self.last_sent_epoch != Some(seq_num)
        {
            if self.next_val_data.is_some() {
                error!("Validator set data is not consumed");
            }
            let locked_epoch = seq_num.get_locked_epoch(self.val_set_update_interval);
            assert_eq!(
                locked_epoch,
                seq_num.to_epoch(self.val_set_update_interval) + Epoch(2)
            );
            self.next_val_data = Some(ValidatorSetUpdate {
                epoch: locked_epoch,
                validator_data: self.genesis_validator_data.clone(),
            });
            self.last_sent_epoch = Some(seq_num);
        }
    }

    fn peek_tick(&self) -> Option<Duration> {
        self.state_root_update.front().map(|(tick, _)| *tick)
    }

    fn step_until(
        &mut self,
        until: Duration,
    ) -> Option<StateRootEvent>
    {
        if self.peek_tick().unwrap_or(Duration::MAX) <= until {
            let (_, event) = self.state_root_update.pop_front().expect("must exist");
            Some(event)
        } else {
            None
        }
    }
}

impl<ST, SCT> MockableStateRootHash for MockStateRootHashScheduler<ST, SCT>
where
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Event = MonadEvent<ST, SCT>;
    type SignatureCollection = SCT;

    fn ready(&self) -> bool {
        self.keep_update && (!self.state_root_update.is_empty() || self.next_val_data.is_some())
    }

    fn get_validator_set_data(&self, _epoch: Epoch) -> ValidatorSetData<Self::SignatureCollection> {
        self.genesis_validator_data.clone()
    }

    fn compute_execution_result(&self, seq_num: &SeqNum) -> Header {
        // hash is pseudorandom seeded by the block's seq num to ensure
        // that it is deterministic between nodes
        let state_root_hash = (self.calc_state_root)(seq_num);
        debug!(
            "block number {:?} state root hash {:?}",
            seq_num.0, state_root_hash
        );

        let mut header = Header::default();
        header.number = seq_num.0;
        header.state_root = state_root_hash.0 .0.into();

        header
    }

    fn set_state_root_poll(&mut self, keep_update: bool) {
        self.keep_update = keep_update;
    }
}

impl<ST, SCT> Executor for MockStateRootHashScheduler<ST, SCT>
where
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Command = StateRootHashCommand<SCT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::CancelBelow(cancel_below) => {
                    while self
                        .state_root_update
                        .front()
                        .is_some_and(|(_, state_root)| state_root.seq_num() < cancel_below)
                    {
                        self.state_root_update.pop_front().unwrap();
                    }
                }
                StateRootHashCommand::RequestProposed(block_id, seq_num, round) => {
                    debug!("GRANT REQUEST PROPOSED {:?}", seq_num.0);
                    self.state_root_update
                        .push_back((Duration::ZERO, StateRootEvent::Proposed(ProposedStateRoot {
                            block_id,
                            seq_num,
                            round,
                            result: self.compute_execution_result(&seq_num),
                        })));
                    self.jank_update_valset(seq_num);
                    wake = true;
                }
                StateRootHashCommand::RequestFinalized(seq_num) => {
                    debug!("GRANT REQUEST FINILIZED {:?}", seq_num.0);
                    self.state_root_update.push_back((Duration::ZERO, StateRootEvent::Finalized(
                        seq_num,
                        self.compute_execution_result(&seq_num),
                    )));
                    self.jank_update_valset(seq_num);
                    wake = true;
                }
                StateRootHashCommand::UpdateValidators(_) => {
                    wake = true;
                }
            }
        }
        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            };
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT> Stream for MockStateRootHashScheduler<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if !this.keep_update {
            debug!("HASH SCHED PENDING 1");
            if this.waker.is_none() {
                this.waker = Some(cx.waker().clone());
            }

            if this.ready() {
                this.waker.take().unwrap().wake();
            }
            return Poll::Pending;
        }

        let event = if let Some((_, event)) = this.state_root_update.pop_front() {
            debug!("HASH SCHED READY {:?}", event);
            Poll::Ready(Some(MonadEvent::StateRootEvent(event)))
        } else if let Some(next_val_data) = this.next_val_data.take() {
            Poll::Ready(Some(MonadEvent::ValidatorEvent(
                monad_executor_glue::ValidatorEvent::<SCT>::UpdateValidators((
                    next_val_data.validator_data,
                    next_val_data.epoch,
                )),
            )))
        } else {
            debug!("HASH SCHED PENDING 2");
            Poll::Pending
        };

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if this.ready() {
            this.waker.take().unwrap().wake();
        }

        event
    }
}
