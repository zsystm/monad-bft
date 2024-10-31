use std::sync::{Arc, Mutex};

use itertools::Either;
use monad_consensus_types::{
    payload::FullTransactionList, signature_collection::SignatureCollection,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;

use super::{event::EthTxPoolEventLoopEvent, pending::PendingEthTxMap};
use crate::storage::EthTxPoolStorage;

pub type SharedEthTxPoolEventLoopState<SCT> = Arc<Mutex<EthTxPoolEventLoopState<SCT>>>;

#[derive(Debug)]
pub struct EthTxPoolEventLoopState<SCT>
where
    SCT: SignatureCollection,
{
    storage: EthTxPoolStorage,
    pending: PendingEthTxMap,
    events: Vec<EthTxPoolEventLoopEvent<SCT>>,
}

impl<SCT> EthTxPoolEventLoopState<SCT>
where
    SCT: SignatureCollection,
{
    pub fn new_shared(block_policy: &EthBlockPolicy) -> SharedEthTxPoolEventLoopState<SCT> {
        Arc::new(Mutex::new(Self {
            storage: EthTxPoolStorage::new(block_policy),
            pending: PendingEthTxMap::default(),
            events: Vec::default(),
        }))
    }

    pub fn add_event(&mut self, event: EthTxPoolEventLoopEvent<SCT>) {
        self.events.push(event);
    }

    pub fn process_all_events(&mut self) {
        self.process_filtered_events(|_| true);

        assert!(self.events.is_empty());
    }

    pub fn create_proposal<SBT>(
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
    {
        self.process_filtered_events(|event| match event {
            EthTxPoolEventLoopEvent::CommittedBlock(_) => true,
            EthTxPoolEventLoopEvent::TxBatch(_) | EthTxPoolEventLoopEvent::Clear => false,
        });

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

    fn process_filtered_events(&mut self, f: fn(&EthTxPoolEventLoopEvent<SCT>) -> bool) {
        for event in std::mem::take(&mut self.events) {
            if f(&event) {
                self.process_event(event);
            } else {
                self.events.push(event);
            }
        }
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
                self.storage.update_committed_block(committed_block);
            }
            EthTxPoolEventLoopEvent::Clear => {
                self.storage.clear();
                self.pending.clear();
            }
        }
    }
}
