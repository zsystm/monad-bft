use std::{
    sync::{Arc, Condvar},
    thread::JoinHandle,
};

use monad_consensus_types::{
    payload::FullTransactionList, signature_collection::SignatureCollection,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_tx::EthTransaction;
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;
use rtrb::{Producer, PushError};
use thiserror::Error;

use super::{
    state::SharedEthTxPoolEventLoopState, EthTxPoolEventLoopError, EthTxPoolEventLoopEvent,
};

/// The event_loop client enforces that the new_event condition variable
/// is notified after an event is added and that txpool thread crashes
/// are detected and bubbled up.
#[derive(Debug)]
pub struct EthTxPoolEventLoopClient<SCT>
where
    SCT: SignatureCollection,
{
    handle: JoinHandle<Result<(), EthTxPoolEventLoopError>>,
    state: SharedEthTxPoolEventLoopState<SCT>,
    events: Producer<EthTxPoolEventLoopEvent<SCT>>,
    new_event: Arc<Condvar>,
}

impl<SCT> EthTxPoolEventLoopClient<SCT>
where
    SCT: SignatureCollection,
{
    pub(super) fn new(
        handle: JoinHandle<Result<(), EthTxPoolEventLoopError>>,
        state: SharedEthTxPoolEventLoopState<SCT>,
        events: Producer<EthTxPoolEventLoopEvent<SCT>>,
        new_event: Arc<Condvar>,
    ) -> Self {
        Self {
            handle,
            state,
            events,
            new_event,
        }
    }

    fn check_handle_health(&self) -> Result<(), EthTxPoolEventLoopClientError> {
        if self.handle.is_finished() || self.events.is_abandoned() {
            return Err(EthTxPoolEventLoopClientError::EventLoopTerminated);
        }

        Ok(())
    }

    fn notify_event(
        &mut self,
        event: EthTxPoolEventLoopEvent<SCT>,
    ) -> Result<(), EthTxPoolEventLoopClientError> {
        self.check_handle_health()?;

        self.events
            .push(event)
            .map_err(|PushError::Full(_)| EthTxPoolEventLoopClientError::EventLoopFrozen)?;

        self.new_event.notify_all();

        Ok(())
    }

    pub fn notify_tx_batch(
        &mut self,
        txs: Vec<EthTransaction>,
    ) -> Result<(), EthTxPoolEventLoopClientError> {
        self.notify_event(EthTxPoolEventLoopEvent::TxBatch(txs))
    }

    pub fn notify_committed_block(
        &mut self,
        committed_block: EthValidatedBlock<SCT>,
    ) -> Result<(), EthTxPoolEventLoopClientError> {
        self.notify_event(EthTxPoolEventLoopEvent::CommittedBlock(committed_block))
    }

    pub fn notify_clear(&mut self) -> Result<(), EthTxPoolEventLoopClientError> {
        self.notify_event(EthTxPoolEventLoopEvent::Clear)
    }

    pub fn create_proposal<SBT>(
        &mut self,
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        proposal_gas_limit: u64,
        block_policy: &EthBlockPolicy,
        extending_blocks: Vec<&EthValidatedBlock<SCT>>,
        state_backend: &SBT,
    ) -> Result<Result<FullTransactionList, StateBackendError>, EthTxPoolEventLoopClientError>
    where
        SBT: StateBackend,
    {
        self.check_handle_health()?;

        let mut state = self
            .state
            .lock()
            .map_err(|_| EthTxPoolEventLoopClientError::PoisonError)?;

        Ok(state.create_proposal(
            &mut self.events,
            proposed_seq_num,
            tx_limit,
            proposal_gas_limit,
            block_policy,
            extending_blocks,
            state_backend,
        ))
    }
}

#[derive(Error, Debug)]
pub enum EthTxPoolEventLoopClientError {
    #[error("EventLoopTerminated")]
    EventLoopTerminated,

    #[error("EventLoopFrozen")]
    EventLoopFrozen,

    #[error("PoisonError")]
    PoisonError,
}
