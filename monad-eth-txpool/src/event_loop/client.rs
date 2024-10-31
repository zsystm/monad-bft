use std::{
    sync::{Arc, Condvar, MutexGuard},
    thread::JoinHandle,
};

use monad_consensus_types::{
    payload::FullTransactionList, signature_collection::SignatureCollection,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_tx::EthTransaction;
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;
use thiserror::Error;

use super::{
    state::{EthTxPoolEventLoopState, SharedEthTxPoolEventLoopState},
    EthTxPoolEventLoopError, EthTxPoolEventLoopEvent,
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
    new_event: Arc<Condvar>,
}

impl<SCT> EthTxPoolEventLoopClient<SCT>
where
    SCT: SignatureCollection,
{
    pub(super) fn new(
        handle: JoinHandle<Result<(), EthTxPoolEventLoopError>>,
        state: SharedEthTxPoolEventLoopState<SCT>,
        new_event: Arc<Condvar>,
    ) -> Self {
        Self {
            handle,
            state,
            new_event,
        }
    }

    pub fn create_proposal<SBT>(
        &self,
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
        let mut state = self.lock_state()?;

        Ok(state.create_proposal(
            proposed_seq_num,
            tx_limit,
            proposal_gas_limit,
            block_policy,
            extending_blocks,
            state_backend,
        ))
    }

    pub fn notify_tx_batch(
        &self,
        txs: Vec<EthTransaction>,
    ) -> Result<(), EthTxPoolEventLoopClientError> {
        let mut state = self.lock_state()?;

        state.add_event(EthTxPoolEventLoopEvent::TxBatch(txs));
        self.new_event.notify_all();

        Ok(())
    }

    pub fn notify_committed_block(
        &self,
        committed_block: EthValidatedBlock<SCT>,
    ) -> Result<(), EthTxPoolEventLoopClientError> {
        let mut state = self.lock_state()?;

        state.add_event(EthTxPoolEventLoopEvent::CommittedBlock(committed_block));
        self.new_event.notify_all();

        Ok(())
    }

    pub fn notify_clear(&self) -> Result<(), EthTxPoolEventLoopClientError> {
        let mut state = self.lock_state()?;

        state.add_event(EthTxPoolEventLoopEvent::Clear);
        self.new_event.notify_all();

        Ok(())
    }

    fn lock_state(
        &self,
    ) -> Result<MutexGuard<EthTxPoolEventLoopState<SCT>>, EthTxPoolEventLoopClientError> {
        if self.handle.is_finished() {
            return Err(EthTxPoolEventLoopClientError::EventLoopTerminated);
        }

        self.state
            .lock()
            .map_err(|_| EthTxPoolEventLoopClientError::PoisonError)
    }
}

#[derive(Error, Debug)]
pub enum EthTxPoolEventLoopClientError {
    #[error("EventLoopTerminated")]
    EventLoopTerminated,

    #[error("PoisonError")]
    PoisonError,
}
