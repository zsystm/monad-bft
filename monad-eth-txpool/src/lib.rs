use std::time::Duration;

use alloy_rlp::Decodable;
use bytes::Bytes;
use monad_consensus_types::{
    payload::FullTransactionList, signature_collection::SignatureCollection, txpool::TxPool,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_tx::{EthSignedTransaction, EthTransaction};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use self::event_loop::{EthTxPoolEventLoop, EthTxPoolEventLoopClient};
pub(self) use self::transaction::ValidEthTransaction;
pub use self::{mock::MockEthTxPool, noop::NoopEthTxPool};

mod event_loop;
mod mock;
mod noop;
mod storage;
mod transaction;

#[derive(Debug)]
pub struct EthTxPool<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    event_loop_client: EthTxPoolEventLoopClient<SCT, SBT>,
}

impl<SCT, SBT> EthTxPool<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend + 'static,
{
    pub fn new(block_policy: &EthBlockPolicy, tx_expiry: Duration) -> Self {
        let event_loop_client = EthTxPoolEventLoop::start(block_policy, tx_expiry);

        Self { event_loop_client }
    }
}

impl<SCT, SBT> TxPool<SCT, EthBlockPolicy, SBT> for EthTxPool<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    fn insert_tx(
        &mut self,
        txns: Vec<Bytes>,
        _block_policy: &EthBlockPolicy,
        _state_backend: &SBT,
    ) -> Vec<Bytes> {
        let (txs, tx_hashes) = txns
            .into_par_iter()
            .filter_map(|raw_tx| {
                let tx = EthSignedTransaction::decode(&mut raw_tx.as_ref()).ok()?;
                let signer = tx.recover_signer().ok()?;
                Some((EthTransaction::new_unchecked(tx, signer), raw_tx))
            })
            .unzip();

        self.event_loop_client
            .notify_tx_batch(txs)
            .expect("tx batch notify succeeds");

        tx_hashes
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
        self.event_loop_client
            .create_proposal(
                proposed_seq_num,
                tx_limit,
                proposal_gas_limit,
                block_policy,
                extending_blocks,
                state_backend,
            )
            .expect("create proposal client task succeeds")
    }

    fn update_committed_block(&mut self, committed_block: &EthValidatedBlock<SCT>) {
        self.event_loop_client
            .notify_committed_block(committed_block.to_owned())
            .expect("committed block notify succeeds");
    }

    fn clear(&mut self) {}

    fn reset(&mut self, _last_delay_committed_blocks: Vec<&EthValidatedBlock<SCT>>) {
        todo!()
    }
}
