use indexmap::IndexMap;
use monad_consensus_types::txpool::TxPoolInsertionError;
use monad_eth_types::EthAddress;

pub use self::list::PendingTxList;
use super::ValidEthTransaction;

mod list;

const MAX_ADDRESSES: usize = 16 * 1024;
const MAX_TXS: usize = 32 * 1024;

#[derive(Clone, Debug, Default)]
pub struct PendingEthTxMap {
    txs: IndexMap<EthAddress, PendingTxList>,
    num_txs: usize,
}

impl PendingEthTxMap {
    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    pub fn num_txs(&self) -> usize {
        self.num_txs
    }

    pub fn addresses(&self) -> impl Iterator<Item = &EthAddress> {
        self.txs.keys()
    }

    pub fn try_add_tx(&mut self, tx: ValidEthTransaction) -> Result<(), TxPoolInsertionError> {
        if self.num_txs >= MAX_TXS {
            return Err(TxPoolInsertionError::PoolFull);
        }

        let num_addresses = self.txs.len();
        assert!(num_addresses <= MAX_ADDRESSES);

        match self.txs.entry(tx.sender()) {
            indexmap::map::Entry::Occupied(mut tx_list) => {
                if tx_list.get_mut().try_add(tx)? {
                    self.num_txs += 1;
                }
            }
            indexmap::map::Entry::Vacant(v) => {
                if num_addresses == MAX_ADDRESSES {
                    return Err(TxPoolInsertionError::PoolFull);
                }

                v.insert(PendingTxList::new(tx));
                self.num_txs += 1;
            }
        }

        Ok(())
    }

    pub fn remove(&mut self, address: &EthAddress) -> Option<PendingTxList> {
        self.txs.swap_remove(address)
    }

    pub fn clear(&mut self) {
        self.txs.clear();
        self.num_txs = 0;
    }
}
