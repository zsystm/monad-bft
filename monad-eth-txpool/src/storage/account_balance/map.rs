use indexmap::{map::Entry as IndexMapEntry, IndexMap};
use monad_eth_types::EthAddress;

#[derive(Clone, Debug, Default)]
pub struct AccountBalanceMap(IndexMap<EthAddress, Option<u128>>);

impl AccountBalanceMap {
    pub fn contains_address(&self, address: &EthAddress) -> bool {
        self.0.contains_key(address)
    }

    pub fn get_account_balance(&self, address: &EthAddress) -> Option<u128> {
        self.0.get(address).cloned().flatten()
    }

    pub fn entry(&mut self, address: EthAddress) -> IndexMapEntry<EthAddress, Option<u128>> {
        self.0.entry(address)
    }
}
