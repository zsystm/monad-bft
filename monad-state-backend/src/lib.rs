use std::sync::{Arc, Mutex};

use monad_eth_types::{EthAccount, EthAddress};
use monad_types::SeqNum;

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
        block: SeqNum,
        addresses: impl Iterator<Item = &'a EthAddress>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        let latest = self.raw_read_latest_block();
        if latest < block {
            // latest < block
            return Err(StateBackendError::NotAvailableYet);
        }
        // block <= latest

        let statuses = addresses
            .map(|address| self.raw_read_account(block, address))
            .collect();

        let earliest = self.raw_read_earliest_block();
        if block < earliest {
            // block < earliest
            return Err(StateBackendError::NeverAvailable);
        }

        // all accounts are now guaranteed to be fully consistent and correct
        Ok(statuses)
    }

    /// Fetches account from storage backend
    /// Must be sequentially consistent
    fn raw_read_account(&self, block: SeqNum, address: &EthAddress) -> Option<EthAccount>;
    /// Fetches earliest block from storage backend
    /// Must be sequentially consistent
    fn raw_read_earliest_block(&self) -> SeqNum;
    /// Fetches latest block from storage backend
    /// Must be sequentially consistent
    fn raw_read_latest_block(&self) -> SeqNum;
}

#[derive(Debug, Default, Clone)]
pub struct NopStateBackend;

impl StateBackend for NopStateBackend {
    fn raw_read_account(&self, _block: SeqNum, _address: &EthAddress) -> Option<EthAccount> {
        None
    }

    fn raw_read_earliest_block(&self) -> SeqNum {
        SeqNum::MIN
    }

    fn raw_read_latest_block(&self) -> SeqNum {
        SeqNum::MAX
    }
}

impl<T: StateBackend> StateBackend for Arc<Mutex<T>> {
    fn raw_read_account(&self, block: SeqNum, address: &EthAddress) -> Option<EthAccount> {
        let state = self.lock().unwrap();
        state.raw_read_account(block, address)
    }

    fn raw_read_earliest_block(&self) -> SeqNum {
        let state = self.lock().unwrap();
        state.raw_read_earliest_block()
    }

    fn raw_read_latest_block(&self) -> SeqNum {
        let state = self.lock().unwrap();
        state.raw_read_latest_block()
    }
}
