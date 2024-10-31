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
use tracing::warn;

use self::event_loop::{EthTxPoolEventLoop, EthTxPoolEventLoopClient};
pub use self::mock::MockEthTxPool;
pub(self) use self::transaction::ValidEthTransaction;

mod event_loop;
mod mock;
mod storage;
mod transaction;

const MAX_PROPOSAL_SIZE: usize = 10_000;
const INSERT_TXS_MIN_PROMOTE: usize = 32;
const INSERT_TXS_MAX_PROMOTE: usize = 128;

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

    // pub fn is_empty(&self) -> bool {
    //     self.pending.is_empty() && self.tracked.is_empty()
    // }

    // pub fn num_txs(&self) -> usize {
    //     self.pending
    //         .num_txs()
    //         .checked_add(self.tracked.num_txs())
    //         .expect("pool size does not overflow")
    // }

    // pub fn promote_pending<SCT, SBT>(
    //     &mut self,
    //     block_policy: &EthBlockPolicy,
    //     state_backend: &SBT,
    //     max_promotable: usize,
    // ) -> Result<(), StateBackendError>
    // where
    //     SCT: SignatureCollection,
    //     SBT: StateBackend,
    // {
    //     self.tracked.promote_pending::<SCT, SBT>(
    //         block_policy,
    //         state_backend,
    //         &mut self.pending,
    //         max_promotable,
    //     )
    // }

    // fn validate_and_insert_tx(
    //     &mut self,
    //     tx: EthTransaction,
    //     block_policy: &EthBlockPolicy,
    //     account_balance: &Balance,
    // ) -> Result<(), TxPoolInsertionError> {
    //     if !self.do_local_insert {
    //         return Ok(());
    //     }

    //     let tx = ValidEthTransaction::validate(tx, block_policy)?;
    //     tx.apply_txn_fee(account_balance)?;

    //     // TODO(andr-dev): Should any additional tx validation occur before inserting into mempool

    //     match self.tracked.try_add_tx(tx) {
    //         Either::Left(tx) => self.pending.try_add_tx(tx),
    //         Either::Right(result) => result,
    //     }
    // }

    // fn validate_and_insert_txs<SCT>(
    //     &mut self,
    //     block_policy: &EthBlockPolicy,
    //     state_backend: &impl StateBackend,
    //     txs: Vec<EthTransaction>,
    // ) -> Result<Vec<Result<(), TxPoolInsertionError>>, StateBackendError>
    // where
    //     SCT: SignatureCollection,
    // {
    //     let senders = txs.iter().map(|tx| EthAddress(tx.signer())).collect_vec();

    //     let block_seq_num = block_policy.get_last_commit() + SeqNum(1); // ?????

    //     let sender_account_balances = block_policy.compute_account_base_balances::<SCT>(
    //         block_seq_num,
    //         state_backend,
    //         None,
    //         senders.iter(),
    //     )?;

    //     let results = txs
    //         .into_iter()
    //         .zip(senders.iter())
    //         .map(|(tx, sender)| {
    //             self.validate_and_insert_tx(
    //                 tx,
    //                 block_policy,
    //                 &sender_account_balances
    //                     .get(&sender)
    //                     .cloned()
    //                     .unwrap_or_default(),
    //             )
    //         })
    //         .collect();
    //     Ok(results)
    // }
    //     }
    // }
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
        // if let Err(state_backend_error) = self.promote_pending::<SCT, SBT>(
        //     block_policy,
        //     state_backend,
        //     txns.len()
        //         .min(INSERT_TXS_MIN_PROMOTE)
        //         .max(INSERT_TXS_MAX_PROMOTE),
        // ) {
        //     if self.pending.is_at_promote_txs_watermark() {
        //         warn!(
        //             ?state_backend_error,
        //             "txpool failed to promote at pending promote txs watermark"
        //         );
        //     }

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

    fn reset(&mut self, last_delay_committed_blocks: Vec<&BPT::ValidatedBlock>) {
        todo!()
    }
}
