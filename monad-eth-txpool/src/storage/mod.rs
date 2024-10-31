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

use self::{pending::PendingEthTxMap, tracked::TrackedEthTxMap, transaction::ValidEthTransaction};

mod pending;
mod tracked;
mod transaction;

#[derive(Clone, Debug)]
pub struct EthTxPoolStorage {
    chain_id: u64,
    pending: PendingEthTxMap,
    tracked: TrackedEthTxMap,
}

impl EthTxPoolStorage {
    pub fn new(block_policy: &EthBlockPolicy) -> Self {
        Self::new_with_chain_id(block_policy.get_chain_id())
    }

    pub(super) fn new_with_chain_id(chain_id: u64) -> Self {
        Self {
            chain_id,
            pending: PendingEthTxMap::default(),
            tracked: TrackedEthTxMap::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.tracked.is_empty()
    }

    pub fn num_txs(&self) -> usize {
        self.pending
            .num_txs()
            .checked_add(self.tracked.num_txs())
            .expect("pool size does not overflow")
    }

    pub fn promote_pending<SCT, SBT>(
        &mut self,
        block_policy: &EthBlockPolicy,
        state_backend: &SBT,
        max_promotable: usize,
    ) -> Result<(), StateBackendError>
    where
        SCT: SignatureCollection,
        SBT: StateBackend,
    {
        self.tracked.promote_pending::<SCT, SBT>(
            block_policy,
            state_backend,
            &mut self.pending,
            max_promotable,
        )
    }

    pub fn insert_tx(&mut self, tx: EthTransaction) -> Result<(), TxPoolInsertionError> {
        let tx = ValidEthTransaction::validate(tx, self.chain_id)?;

        match self.tracked.try_add_tx(tx) {
            Either::Left(tx) => self.pending.try_add_tx(tx),
            Either::Right(result) => result,
        }
    }

    pub fn create_proposal<SCT, SBT>(
        &mut self,
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        proposal_gas_limit: u64,
        block_policy: &EthBlockPolicy,
        extending_blocks: Vec<&EthValidatedBlock<SCT>>,
        state_backend: &SBT,
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
            &mut self.pending,
        )
    }

    pub fn update_committed_block<SCT>(&mut self, committed_block: EthValidatedBlock<SCT>)
    where
        SCT: SignatureCollection,
    {
        self.tracked.update_committed_block(committed_block);
    }

    pub fn clear(&mut self) {
        self.pending.clear();
        self.tracked.clear();
    }
}
