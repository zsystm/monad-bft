use std::time::Duration;

use itertools::Either;
use monad_consensus_types::{
    payload::FullTransactionList, signature_collection::SignatureCollection,
    txpool::TxPoolInsertionError,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_tx::EthTransaction;
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;
use tracing::error;
use tracked::TrackedTxMap;

use crate::{event_loop::pending::PendingTxMap, ValidEthTransaction};

mod tracked;

#[derive(Clone, Debug)]
pub struct EthTxPoolStorage<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    chain_id: u64,
    tracked: TrackedTxMap<SCT, SBT>,
}

impl<SCT, SBT> EthTxPoolStorage<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    pub fn new(block_policy: &EthBlockPolicy, tx_expiry: Duration) -> Self {
        Self::new_with_chain_id(block_policy.get_chain_id(), tx_expiry)
    }

    pub fn new_with_chain_id(chain_id: u64, tx_expiry: Duration) -> Self {
        Self {
            chain_id,
            tracked: TrackedTxMap::new(tx_expiry),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tracked.is_empty()
    }

    pub fn num_txs(&self) -> usize {
        self.tracked.num_txs()
    }

    pub fn try_add_tx(
        &mut self,
        tx: EthTransaction,
    ) -> Either<ValidEthTransaction, Result<(), TxPoolInsertionError>> {
        match ValidEthTransaction::validate(tx, self.chain_id) {
            Ok(tx) => self.try_add_validated_tx(tx),
            Err(e) => Either::Right(Err(e)),
        }
    }

    pub fn try_add_validated_tx(
        &mut self,
        tx: ValidEthTransaction,
    ) -> Either<ValidEthTransaction, Result<(), TxPoolInsertionError>> {
        self.tracked.try_add_tx(tx)
    }

    pub fn create_proposal(
        &mut self,
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        proposal_gas_limit: u64,
        block_policy: &EthBlockPolicy,
        pending_blocks: Vec<&EthValidatedBlock<SCT>>,
        state_backend: &SBT,
        pending: &mut PendingTxMap,
    ) -> Result<FullTransactionList, StateBackendError> {
        if block_policy.get_chain_id() != self.chain_id {
            error!("txpool attempted to create proposal with invalid chain id");
            return Err(StateBackendError::NeverAvailable);
        }

        self.tracked.create_proposal(
            proposed_seq_num,
            tx_limit,
            proposal_gas_limit,
            block_policy,
            pending_blocks,
            state_backend,
            pending,
        )
    }

    pub fn update_committed_block(
        &mut self,
        committed_block: EthValidatedBlock<SCT>,
        pending: &mut PendingTxMap,
    ) where
        SCT: SignatureCollection,
    {
        self.tracked
            .update_committed_block(committed_block, pending);
    }
}
