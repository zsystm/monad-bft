use std::time::Duration;

use alloy_rlp::Decodable;
use bytes::Bytes;
use itertools::{Either, Itertools};
use monad_consensus_types::{
    payload::FullTransactionList, signature_collection::SignatureCollection, txpool::TxPool,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_tx::{EthSignedTransaction, EthTransaction};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;

use crate::{event_loop::pending::PendingTxMap, storage::EthTxPoolStorage};

#[derive(Clone, Debug)]
pub struct MockEthTxPool<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    storage: EthTxPoolStorage<SCT, SBT>,
    pending: PendingTxMap,
}

impl<SCT, SBT> MockEthTxPool<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    pub fn new(block_policy: &EthBlockPolicy) -> Self {
        Self {
            storage: EthTxPoolStorage::new(block_policy, Duration::MAX),
            pending: PendingTxMap::default(),
        }
    }

    pub fn new_with_chain_id(chain_id: u64) -> Self {
        Self {
            storage: EthTxPoolStorage::new_with_chain_id(chain_id, Duration::MAX),
            pending: PendingTxMap::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    pub fn num_txs(&self) -> usize {
        self.storage.num_txs()
    }
}

impl<SCT, SBT> TxPool<SCT, EthBlockPolicy, SBT> for MockEthTxPool<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    fn insert_tx(
        &mut self,
        txs: Vec<Bytes>,
        _block_policy: &EthBlockPolicy,
        _state_backend: &SBT,
    ) -> Vec<Bytes> {
        txs.into_iter()
            .filter_map(|b| {
                let tx = EthSignedTransaction::decode(&mut b.as_ref()).ok()?;
                let signer = tx.recover_signer().ok()?;

                let tx = EthTransaction::new_unchecked(tx, signer);

                match self.storage.try_add_tx(tx) {
                    Either::Left(tx) => self.pending.try_add_tx(tx).ok().map(|_| b),
                    Either::Right(Ok(())) => Some(b),
                    Either::Right(Err(_)) => None,
                }
            })
            .collect_vec()
    }

    fn create_proposal(
        &mut self,
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        proposal_gas_limit: u64,
        block_policy: &EthBlockPolicy,
        pending_blocks: Vec<&EthValidatedBlock<SCT>>,
        state_backend: &SBT,
    ) -> Result<FullTransactionList, StateBackendError> {
        self.storage.create_proposal(
            proposed_seq_num,
            tx_limit,
            proposal_gas_limit,
            block_policy,
            pending_blocks,
            state_backend,
            &mut self.pending,
        )
    }

    fn update_committed_block(&mut self, committed_block: &EthValidatedBlock<SCT>) {
        self.storage
            .update_committed_block(committed_block.to_owned(), &mut self.pending)
    }

    fn clear(&mut self) {}

    fn reset(&mut self, last_delay_committed_blocks: Vec<&EthValidatedBlock<SCT>>) {
        todo!()
    }
}
