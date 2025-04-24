use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use alloy_primitives::Address;
use monad_eth_types::{EthAccount, Nonce};
use monad_types::{BlockId, Round, SeqNum};

pub use self::in_memory::{InMemoryBlockState, InMemoryState, InMemoryStateInner};

mod in_memory;

#[derive(Debug, PartialEq)]
pub enum StateBackendError {
    /// not available yet
    NotAvailableYet,
    /// will never be available
    NeverAvailable,
}

/// Backend provider of account data: balance and nonce
pub trait StateBackend {
    fn get_account_statuses<'a>(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        round: &Round,
        is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError>;

    /// Fetches earliest block from storage backend
    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum>;
    /// Fetches latest block from storage backend
    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum>;

    fn total_db_lookups(&self) -> u64;
}

pub trait StateBackendTest {
    fn ledger_propose(
        &mut self,
        block_id: BlockId,
        seq_num: SeqNum,
        round: Round,
        parent_round: Round,
        new_account_nonces: BTreeMap<Address, Nonce>,
    );

    fn ledger_commit(&mut self, block_id: &BlockId);
}

impl<T: StateBackend> StateBackend for Arc<Mutex<T>> {
    fn get_account_statuses<'a>(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        round: &Round,
        is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        let state = self.lock().unwrap();
        state.get_account_statuses(block_id, seq_num, round, is_finalized, addresses)
    }

    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum> {
        let state = self.lock().unwrap();
        state.raw_read_earliest_finalized_block()
    }

    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum> {
        let state = self.lock().unwrap();
        state.raw_read_latest_finalized_block()
    }

    fn total_db_lookups(&self) -> u64 {
        self.lock().unwrap().total_db_lookups()
    }
}

impl<T: StateBackendTest> StateBackendTest for Arc<Mutex<T>> {
    fn ledger_commit(&mut self, block_id: &BlockId) {
        let mut state = self.lock().unwrap();
        state.ledger_commit(block_id);
    }

    fn ledger_propose(
        &mut self,
        block_id: BlockId,
        seq_num: SeqNum,
        round: Round,
        parent_round: Round,
        new_account_nonces: BTreeMap<Address, Nonce>,
    ) {
        let mut state = self.lock().unwrap();
        state.ledger_propose(block_id, seq_num, round, parent_round, new_account_nonces);
    }
}
