use bytes::Bytes;
use monad_consensus_types::{
    payload::FullTransactionList, signature_collection::SignatureCollection, txpool::TxPool,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;

#[derive(Debug, Default)]
pub struct NoopEthTxPool;

impl<SCT, SBT> TxPool<SCT, EthBlockPolicy, SBT> for NoopEthTxPool
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    fn insert_tx(&mut self, _: Vec<Bytes>, _: &EthBlockPolicy, _: &SBT) -> Vec<Bytes> {
        Vec::default()
    }

    fn create_proposal(
        &mut self,
        _: SeqNum,
        _: usize,
        _: u64,
        _: &EthBlockPolicy,
        _: Vec<&EthValidatedBlock<SCT>>,
        _: &SBT,
    ) -> Result<FullTransactionList, StateBackendError> {
        panic!("NoopEthTxPool cannot create proposal!");
    }

    fn update_committed_block(&mut self, _: &EthValidatedBlock<SCT>) {}

    fn clear(&mut self) {}

    fn reset(&mut self, _: Vec<&EthValidatedBlock<SCT>>) {}
}
