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
pub struct EthTxPoolStorage {
    chain_id: u64,
    tracked: TrackedTxMap,
}

impl EthTxPoolStorage {
    pub fn new(block_policy: &EthBlockPolicy) -> Self {
        Self::new_with_chain_id(block_policy.get_chain_id())
    }

    pub(super) fn new_with_chain_id(chain_id: u64) -> Self {
        Self {
            chain_id,
            tracked: TrackedTxMap::default(),
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

    pub fn create_proposal<SCT, SBT>(
        &mut self,
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        proposal_gas_limit: u64,
        block_policy: &EthBlockPolicy,
        extending_blocks: Vec<&EthValidatedBlock<SCT>>,
        state_backend: &SBT,
        pending: &mut PendingTxMap,
    ) -> Result<FullTransactionList, StateBackendError>
    where
        SBT: StateBackend,
        SCT: SignatureCollection,
    {
        if block_policy.get_chain_id() != self.chain_id {
            error!("txpool attempted to create proposal with invalid chain id");
            return Err(StateBackendError::NeverAvailable);
        }

        self.tracked.create_proposal(
            proposed_seq_num,
            tx_limit,
            proposal_gas_limit,
            block_policy,
            extending_blocks,
            state_backend,
            pending,
        )
    }

    pub fn update_committed_block<SCT>(&mut self, committed_block: EthValidatedBlock<SCT>)
    where
        SCT: SignatureCollection,
    {
        self.tracked.update_committed_block(committed_block);
    }
}
