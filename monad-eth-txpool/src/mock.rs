use alloy_rlp::Decodable;
use bytes::Bytes;
use itertools::{Either, Itertools};
use monad_consensus_types::{
    payload::FullTransactionList, signature_collection::SignatureCollection, txpool::TxPool,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_tx::EthTransaction;
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;

use crate::{event_loop::PendingEthTxMap, storage::EthTxPoolStorage, MAX_PROPOSAL_SIZE};

#[derive(Clone, Debug)]
pub struct MockEthTxPool {
    storage: EthTxPoolStorage,
    pending: PendingEthTxMap,
}

impl MockEthTxPool {
    pub fn new(block_policy: &EthBlockPolicy) -> Self {
        Self {
            storage: EthTxPoolStorage::new(block_policy),
            pending: PendingEthTxMap::default(),
        }
    }

    pub fn new_with_chain_id(chain_id: u64) -> Self {
        Self {
            storage: EthTxPoolStorage::new_with_chain_id(chain_id),
            pending: PendingEthTxMap::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    pub fn num_txs(&self) -> usize {
        self.storage.num_txs()
    }
}

impl<SCT, SBT> TxPool<SCT, EthBlockPolicy, SBT> for MockEthTxPool
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
                let tx = EthTransaction::decode(&mut b.as_ref()).ok()?;

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
        extending_blocks: Vec<&EthValidatedBlock<SCT>>,
        state_backend: &SBT,
    ) -> Result<FullTransactionList, StateBackendError> {
        self.storage.create_proposal(
            proposed_seq_num,
            tx_limit.min(MAX_PROPOSAL_SIZE),
            proposal_gas_limit,
            block_policy,
            extending_blocks,
            state_backend,
            &mut self.pending,
        )
    }

    fn update_committed_block(&mut self, committed_block: &EthValidatedBlock<SCT>) {
        self.storage
            .update_committed_block(committed_block.to_owned())
    }

    fn clear(&mut self) {
        self.storage.clear();
    }
}
