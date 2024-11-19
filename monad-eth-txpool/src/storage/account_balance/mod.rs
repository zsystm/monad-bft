use std::collections::BTreeMap;

use indexmap::map::Entry as IndexMapEntry;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_eth_block_policy::EthValidatedBlock;
use monad_eth_types::EthAddress;
use monad_types::SeqNum;

pub use self::{map::AccountBalanceMap, state_backend::AccountBalanceCacheStateBackend};

mod map;
mod state_backend;

#[derive(Clone, Debug, Default)]
pub struct AccountBalanceCache {
    map: BTreeMap<SeqNum, AccountBalanceMap>,
}

impl AccountBalanceCache {
    pub fn get(&self, seqnum: &SeqNum) -> Option<&AccountBalanceMap> {
        self.map.get(seqnum)
    }

    pub fn update(&mut self, seqnum: SeqNum, address: EthAddress, account_balance: Option<u128>) {
        match self.map.entry(seqnum).or_default().entry(address) {
            IndexMapEntry::Occupied(o) => {
                assert_eq!(o.get(), &account_balance, "account balance does not change");
            }
            IndexMapEntry::Vacant(v) => {
                v.insert(account_balance);
            }
        }
    }

    pub fn update_committed_block<SCT>(
        &mut self,
        committed_block: &EthValidatedBlock<SCT>,
        execution_delay: SeqNum,
    ) where
        SCT: SignatureCollection,
    {
        let seqnum = committed_block.block.qc.get_seq_num();

        self.map = self.map.split_off(&SeqNum(
            seqnum
                .0
                .checked_add(1)
                .expect("seqnum does not overflow")
                .saturating_sub(execution_delay.0),
        ));
    }
}
