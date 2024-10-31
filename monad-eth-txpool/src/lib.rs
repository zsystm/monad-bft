use alloy_rlp::Decodable;
use bytes::Bytes;
use monad_consensus_types::{
    payload::FullTransactionList, signature_collection::SignatureCollection, txpool::TxPool,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_tx::EthTransaction;
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use self::event_loop::{EthTxPoolEventLoop, EthTxPoolEventLoopClient};
pub use self::mock::MockEthTxPool;

mod event_loop;
mod mock;
mod storage;

pub(crate) const MAX_PROPOSAL_SIZE: usize = 10_000;

#[derive(Debug)]
pub struct EthTxPool<SCT>
where
    SCT: SignatureCollection,
{
    event_loop_client: EthTxPoolEventLoopClient<SCT>,
    do_local_insert: bool,
}

impl<SCT> EthTxPool<SCT>
where
    SCT: SignatureCollection,
{
    pub fn new(block_policy: &EthBlockPolicy, do_local_insert: bool) -> Self {
        let event_loop_client = EthTxPoolEventLoop::start(block_policy);

        Self {
            event_loop_client,
            do_local_insert,
        }
    }
}

impl<SCT, SBT> TxPool<SCT, EthBlockPolicy, SBT> for EthTxPool<SCT>
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
        if !self.do_local_insert {
            return Vec::default();
        }

        // TODO: unwrap can be removed when this is made generic over the actual
        // tx type rather than Bytes and decoding won't be necessary
        // TODO(rene): sender recovery is done inline here
        let (txs, tx_hashes) = txns
            .into_par_iter()
            .filter_map(|b| {
                EthTransaction::decode(&mut b.as_ref())
                    .ok()
                    .map(|valid_tx| (valid_tx, b))
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
                tx_limit.min(MAX_PROPOSAL_SIZE),
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

    fn clear(&mut self) {
        self.event_loop_client
            .notify_clear()
            .expect("clear notify succeeds")
    }
}
