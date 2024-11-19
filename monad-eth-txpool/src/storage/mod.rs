use itertools::Either;
use monad_consensus_types::{
    payload::FullTransactionList, signature_collection::SignatureCollection,
    txpool::TxPoolInsertionError,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_tx::EthTransaction;
use monad_eth_types::{EthAccount, EthAddress};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;
use tracing::{error, warn};

use self::{
    account_balance::AccountBalanceCache, pending::PendingEthTxMap, tracked::TrackedEthTxMap,
    transaction::ValidEthTransaction,
};

mod account_balance;
mod pending;
mod tracked;
mod transaction;

#[derive(Clone, Debug)]
pub struct EthTxPoolStorage {
    chain_id: u64,
    execution_delay: SeqNum,
    pending: PendingEthTxMap,
    tracked: TrackedEthTxMap,
    account_balance: AccountBalanceCache,
}

impl EthTxPoolStorage {
    pub fn new(block_policy: &EthBlockPolicy) -> Self {
        Self::new_with_chain_id_and_execution_delay(
            block_policy.get_chain_id(),
            block_policy.get_execution_delay(),
        )
    }

    pub(super) fn new_with_chain_id_and_execution_delay(
        chain_id: u64,
        execution_delay: SeqNum,
    ) -> Self {
        Self {
            chain_id,
            execution_delay,
            pending: PendingEthTxMap::default(),
            tracked: TrackedEthTxMap::default(),
            account_balance: AccountBalanceCache::default(),
        }
    }

    pub fn get_execution_delay(&self) -> SeqNum {
        self.execution_delay
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

    pub fn last_commit_seq_num(&self) -> SeqNum {
        self.tracked.last_commit_seq_num()
    }

    pub fn iter_pending_addresses(&self) -> impl Iterator<Item = &EthAddress> {
        self.pending.addresses()
    }

    pub fn get_account_balance_addresses(&self, seqnum: SeqNum, limit: usize) -> Vec<EthAddress> {
        let Some(account_balance_map) = self.account_balance.get(&seqnum) else {
            return self.tracked.iter_addresses().take(limit).cloned().collect();
        };

        self.tracked
            .iter_addresses()
            .filter(|address| !account_balance_map.contains_address(address))
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn insert_tx(&mut self, tx: EthTransaction) -> Result<(), TxPoolInsertionError> {
        let tx = ValidEthTransaction::validate(tx, self.chain_id)?;

        match self.tracked.try_add_tx(tx) {
            Either::Left(tx) => self.pending.try_add_tx(tx),
            Either::Right(result) => result,
        }
    }

    pub fn insert_tracked(
        &mut self,
        seqnum: SeqNum,
        tracked: impl Iterator<Item = (EthAddress, Option<EthAccount>)>,
    ) {
        let current_seqnum = self.tracked.last_commit_seq_num();

        if current_seqnum != seqnum {
            warn!(?current_seqnum, ?seqnum, "txpool tracked insert delayed");
            return;
        }

        for (address, account) in tracked {
            let Some(pending) = self.pending.remove(&address) else {
                warn!("txpool pending account dissapeared :(");
                continue;
            };

            let Some(account) = account else {
                continue;
            };

            self.tracked.insert(address, account.nonce, pending);
        }
    }

    pub fn insert_account_balances(
        &mut self,
        seqnum: SeqNum,
        account_balances: impl Iterator<Item = (EthAddress, Option<u128>)>,
    ) {
        for (address, account_balance) in account_balances {
            self.account_balance
                .update(seqnum, address, account_balance);
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
            &self.account_balance,
        )
    }

    pub fn update_committed_block<SCT>(&mut self, committed_block: EthValidatedBlock<SCT>)
    where
        SCT: SignatureCollection,
    {
        self.account_balance
            .update_committed_block(&committed_block, self.execution_delay);

        self.tracked.update_committed_block(committed_block);
    }

    pub fn clear(&mut self) {
        self.pending.clear();
        self.tracked.clear();
    }
}
