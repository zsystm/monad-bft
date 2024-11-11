use std::sync::{Arc, Mutex};

use monad_consensus_types::{
    payload::FullTransactionList, signature_collection::SignatureCollection,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::{Round, SeqNum};
use tracing::info;

use super::event::EthTxPoolEventLoopEvent;
use crate::storage::EthTxPoolStorage;

pub type SharedEthTxPoolEventLoopState<SCT> = Arc<Mutex<EthTxPoolEventLoopState<SCT>>>;

#[derive(Debug)]
pub struct EthTxPoolEventLoopState<SCT>
where
    SCT: SignatureCollection,
{
    storage: EthTxPoolStorage,
    events: Vec<EthTxPoolEventLoopEvent<SCT>>,
    current_round: Round,
    next_leader_round: Option<Round>,
}

impl<SCT> EthTxPoolEventLoopState<SCT>
where
    SCT: SignatureCollection,
{
    pub fn new_shared(block_policy: &EthBlockPolicy) -> SharedEthTxPoolEventLoopState<SCT> {
        Arc::new(Mutex::new(Self {
            storage: EthTxPoolStorage::new(block_policy),
            events: Vec::default(),
            current_round: Round(0),
            next_leader_round: None,
        }))
    }

    pub fn add_event(&mut self, event: EthTxPoolEventLoopEvent<SCT>) {
        self.events.push(event);
    }

    pub fn process_all_events(&mut self) {
        self.process_filtered_events(|_| true);
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
            EthTxPoolEventLoopEvent::CommittedBlock(_)
            | EthTxPoolEventLoopEvent::RoundUpdate {
                current_round: _,
                next_leader_round: _,
            } => true,
            EthTxPoolEventLoopEvent::TxBatch(_) => false,
        });

        self.storage.create_proposal(
            proposed_seq_num,
            tx_limit,
            proposal_gas_limit,
            block_policy,
            extending_blocks,
            state_backend,
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
                    // TODO(andr-dev): Txpool metrics
                    let _ = self.storage.insert_tx(tx);
                }
            }
            EthTxPoolEventLoopEvent::CommittedBlock(committed_block) => {
                self.storage.update_committed_block(committed_block);
            }
            EthTxPoolEventLoopEvent::RoundUpdate {
                current_round: new_current_round,
                next_leader_round: new_next_leader_round,
            } => {
                if let Some(next_leader_round) = self.next_leader_round {
                    if self.current_round == next_leader_round
                        && self
                            .current_round
                            .0
                            .checked_add(1)
                            .expect("round number does not overflow")
                            == new_current_round.0
                    {
                        info!("txpool fake clear");
                        self.storage.clear();
                    }
                }

                self.current_round = new_current_round;
                self.next_leader_round = new_next_leader_round;
            }
        }
    }
}
