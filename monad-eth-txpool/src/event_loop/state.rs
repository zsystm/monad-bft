use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use itertools::{Either, Itertools};
use monad_consensus_types::{
    payload::FullTransactionList, signature_collection::SignatureCollection,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;
use rtrb::{Consumer, Producer};

use super::{event::EthTxPoolEventLoopEvent, pending::PendingTxMap};
use crate::storage::EthTxPoolStorage;

pub type SharedEthTxPoolEventLoopState<SCT, SBT> = Arc<Mutex<EthTxPoolEventLoopState<SCT, SBT>>>;

#[derive(Debug)]
pub struct EthTxPoolEventLoopState<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    storage: EthTxPoolStorage<SCT, SBT>,
    pending: PendingTxMap,
    events: Consumer<EthTxPoolEventLoopEvent<SCT>>,
}

impl<SCT, SBT> EthTxPoolEventLoopState<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    pub fn new_shared(
        block_policy: &EthBlockPolicy,
        tx_expiry: Duration,
        events: Consumer<EthTxPoolEventLoopEvent<SCT>>,
    ) -> SharedEthTxPoolEventLoopState<SCT, SBT> {
        Arc::new(Mutex::new(Self {
            storage: EthTxPoolStorage::new(block_policy, tx_expiry),
            pending: PendingTxMap::default(),
            events,
        }))
    }

    pub fn process_all_events(&mut self) {
        while let Ok(event) = self.events.pop() {
            self.process_event(event);
        }
    }

    pub fn create_proposal(
        &mut self,
        events_tx: &mut Producer<EthTxPoolEventLoopEvent<SCT>>,
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        proposal_gas_limit: u64,
        block_policy: &EthBlockPolicy,
        extending_blocks: Vec<&EthValidatedBlock<SCT>>,
        state_backend: &SBT,
    ) -> Result<FullTransactionList, StateBackendError> {
        for event in self
            .events
            .read_chunk(self.events.slots())
            .expect("read chunk never fails")
            .into_iter()
            .collect_vec()
        {
            match event {
                event @ EthTxPoolEventLoopEvent::CommittedBlock(_) => {
                    self.process_event(event);
                }
                event @ EthTxPoolEventLoopEvent::TxBatch(_) => {
                    // TODO(andr-dev): Add test for full channel
                    events_tx.push(event).expect("channel never overflows");
                }
            }
        }

        self.storage.create_proposal(
            proposed_seq_num,
            tx_limit,
            proposal_gas_limit,
            block_policy,
            extending_blocks,
            state_backend,
            &mut self.pending,
        )
    }

    fn process_event(&mut self, event: EthTxPoolEventLoopEvent<SCT>) {
        match event {
            EthTxPoolEventLoopEvent::TxBatch(txs) => {
                for tx in txs {
                    match self.storage.try_add_tx(tx) {
                        Either::Left(tx) => {
                            if let Err(_) = self.pending.try_add_tx(tx) {
                                // TODO(andr-dev): Txpool metrics
                            }
                        }
                        Either::Right(Ok(())) => {
                            // TODO(andr-dev): Txpool metrics
                        }
                        Either::Right(Err(_)) => {
                            // TODO(andr-dev): Txpool metrics
                        }
                    }
                }
            }
            EthTxPoolEventLoopEvent::CommittedBlock(committed_block) => {
                self.storage
                    .update_committed_block(committed_block, &mut self.pending);
            }
        }
    }
}
