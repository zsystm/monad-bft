use monad_eth_types::{EthAccount, EthAddress};
use monad_state_backend::StateBackend;
use monad_types::SeqNum;

use super::AccountBalanceCache;

pub struct AccountBalanceCacheStateBackend<'a, SBT> {
    cache: &'a AccountBalanceCache,
    state_backend: &'a SBT,
}

impl<'a, SBT> AccountBalanceCacheStateBackend<'a, SBT> {
    pub fn new(cache: &'a AccountBalanceCache, state_backend: &'a SBT) -> Self {
        Self {
            cache,
            state_backend,
        }
    }
}

impl<'a, SBT> StateBackend for AccountBalanceCacheStateBackend<'a, SBT>
where
    SBT: StateBackend,
{
    fn raw_read_account(&self, block: SeqNum, address: &EthAddress) -> Option<EthAccount> {
        self.state_backend.raw_read_account(block, address)
    }

    fn raw_read_balance(&self, block: SeqNum, address: &EthAddress) -> Option<u128> {
        if let Some(account_balance) = self
            .cache
            .get(&block)
            .and_then(|map| map.get_account_balance(address))
        {
            return Some(account_balance);
        }

        self.raw_read_account(block, address)
            .map(|account| account.balance)
    }

    fn raw_read_earliest_block(&self) -> monad_types::SeqNum {
        self.state_backend.raw_read_earliest_block()
    }

    fn raw_read_latest_block(&self) -> monad_types::SeqNum {
        self.state_backend.raw_read_latest_block()
    }
}
