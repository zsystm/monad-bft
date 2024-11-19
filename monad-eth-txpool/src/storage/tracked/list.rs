use std::collections::{btree_map, BTreeMap};

use monad_consensus_types::txpool::TxPoolInsertionError;
use monad_eth_types::{EthAddress, Nonce};

use super::{PendingTxList, ValidEthTransaction};

#[derive(Clone, Debug, Default)]
pub struct TrackedTxList {
    account_nonce: Nonce,
    txs: BTreeMap<Nonce, ValidEthTransaction>,
}

impl TrackedTxList {
    pub fn new_from_account_nonce_and_pending(account_nonce: u64, tx_list: PendingTxList) -> Self {
        Self {
            account_nonce,
            txs: tx_list.into_map().split_off(&account_nonce),
        }
    }

    pub fn num_txs(&self) -> usize {
        self.txs.len()
    }

    pub fn account_nonce(&self) -> Nonce {
        self.account_nonce
    }

    pub fn get_queued(
        &self,
        pending_account_nonce: Option<u64>,
    ) -> impl Iterator<Item = &ValidEthTransaction> {
        let mut account_nonce = pending_account_nonce.unwrap_or(self.account_nonce);

        self.txs
            .range(account_nonce..)
            .map_while(move |(tx_nonce, tx)| {
                debug_assert_eq!(*tx_nonce, tx.nonce());

                if *tx_nonce != account_nonce {
                    return None;
                }

                account_nonce += 1;
                Some(tx)
            })
    }

    pub(crate) fn try_add_tx(
        &mut self,
        tx: ValidEthTransaction,
    ) -> Result<(), TxPoolInsertionError> {
        if tx.nonce() < self.account_nonce {
            return Err(TxPoolInsertionError::NonceTooLow);
        }

        match self.txs.entry(tx.nonce()) {
            btree_map::Entry::Vacant(v) => {
                v.insert(tx);
            }
            btree_map::Entry::Occupied(mut existing_tx) => {
                // TODO(andr-dev): Handle dupliate case for metrics

                if &tx < existing_tx.get() {
                    return Err(TxPoolInsertionError::ExistingHigherPriority);
                }

                existing_tx.insert(tx);
            }
        }

        Ok(())
    }

    pub fn update_account_nonce(
        mut this: indexmap::map::OccupiedEntry<'_, EthAddress, TrackedTxList>,
        account_nonce: &u64,
    ) {
        this.get_mut().account_nonce = *account_nonce;

        let Some((lowest_nonce, _)) = this.get().txs.first_key_value() else {
            return;
        };

        if lowest_nonce >= account_nonce {
            return;
        }

        let txs = this.get_mut().txs.split_off(account_nonce);

        this.get_mut().txs = txs;
    }
}
