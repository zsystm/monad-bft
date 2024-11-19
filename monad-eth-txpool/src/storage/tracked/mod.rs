use std::{collections::BTreeMap, time::Duration};

use heapless::BinaryHeap;
use indexmap::{map::Entry as IndexMapEntry, IndexMap};
use itertools::Either;
use monad_consensus_types::{
    block::BlockType, payload::FullTransactionList, signature_collection::SignatureCollection,
    txpool::TxPoolInsertionError,
};
use monad_eth_block_policy::{AccountNonceRetrievable, EthBlockPolicy, EthValidatedBlock};
use monad_eth_tx::{EthFullTransactionList, EthTransaction};
use monad_eth_types::EthAddress;
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::{DropTimer, SeqNum};
use tracing::{debug, error, info, trace};

use self::list::TrackedTxList;
pub use super::pending::PendingTxList;
use super::{account_balance::AccountBalanceCache, ValidEthTransaction};
use crate::storage::account_balance::AccountBalanceCacheStateBackend;

mod list;

const MAX_ADDRESSES: usize = 1024 * 16;

#[derive(Clone, Debug)]
pub struct TrackedEthTxMap {
    txs: IndexMap<EthAddress, TrackedTxList>,
    last_commit_seq_num: SeqNum,
}

impl Default for TrackedEthTxMap {
    fn default() -> Self {
        Self {
            txs: IndexMap::default(),
            last_commit_seq_num: SeqNum::MIN,
        }
    }
}

impl TrackedEthTxMap {
    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    pub fn num_txs(&self) -> usize {
        self.txs.values().map(TrackedTxList::num_txs).sum()
    }

    pub fn last_commit_seq_num(&self) -> SeqNum {
        self.last_commit_seq_num
    }

    pub fn iter_addresses(&self) -> impl Iterator<Item = &EthAddress> {
        self.txs.keys()
    }

    pub fn try_add_tx(
        &mut self,
        tx: ValidEthTransaction,
    ) -> Either<ValidEthTransaction, Result<(), TxPoolInsertionError>> {
        match self.txs.entry(tx.sender()) {
            IndexMapEntry::Vacant(_) => Either::Left(tx),
            IndexMapEntry::Occupied(mut o) => Either::Right(o.get_mut().try_add_tx(tx)),
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
        account_balance_cache: &AccountBalanceCache,
    ) -> Result<FullTransactionList, StateBackendError>
    where
        SCT: SignatureCollection,
        SBT: StateBackend,
    {
        assert!(
            block_policy.get_last_commit() >= self.last_commit_seq_num,
            "txpool received block policy with lower committed seq num"
        );

        if self.last_commit_seq_num != block_policy.get_last_commit() {
            error!(
                block_policy_last_commit = block_policy.get_last_commit().0,
                txpool_last_commit = self.last_commit_seq_num.0,
                "last commit update does not match block policy last commit"
            );

            return Ok(FullTransactionList::empty());
        }

        let _timer = DropTimer::start(Duration::ZERO, |elapsed| {
            debug!(?elapsed, "txpool create_proposal");
        });

        let state_backend =
            AccountBalanceCacheStateBackend::new(&account_balance_cache, state_backend);

        let account_balances = block_policy.compute_account_base_balances(
            proposed_seq_num,
            &state_backend,
            Some(&extending_blocks),
            self.txs.keys(),
        )?;

        let pending_account_nonces = extending_blocks.get_account_nonces();

        let (proposal_total_gas, proposal_tx_list) = self.create_proposal_tx_list(
            tx_limit,
            proposal_gas_limit,
            account_balances,
            pending_account_nonces,
        )?;

        let proposal_num_tx = proposal_tx_list.len();
        let proposal_rlp_tx_list = EthFullTransactionList(proposal_tx_list).rlp_encode();

        info!(
            ?proposed_seq_num,
            ?proposal_num_tx,
            proposal_total_gas,
            proposal_tx_bytes = proposal_rlp_tx_list.len(),
            "created proposal"
        );

        Ok(FullTransactionList::new(proposal_rlp_tx_list))
    }

    fn create_proposal_tx_list(
        &self,
        tx_limit: usize,
        proposal_gas_limit: u64,
        mut account_balances: BTreeMap<&EthAddress, u128>,
        pending_account_nonces: BTreeMap<EthAddress, u64>,
    ) -> Result<(u64, Vec<EthTransaction>), StateBackendError> {
        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
        struct VirtualValidEthTransaction<'a> {
            tx: &'a ValidEthTransaction,
            virtual_time: u64,
        }

        let mut tx_heap = BinaryHeap::<
            VirtualValidEthTransaction,
            heapless::binary_heap::Max,
            MAX_ADDRESSES,
        >::new();

        let mut virtual_time = 0u64;

        let mut tx_iters = self
            .txs
            .iter()
            .flat_map(|(address, tx_list)| {
                let mut queued = tx_list
                    .get_queued(pending_account_nonces.get(address).cloned())
                    .peekable();

                let tx = queued.next()?;

                if tx_heap
                    .push(VirtualValidEthTransaction { tx, virtual_time })
                    .is_err()
                {
                    // TODO(andr-dev): Warn as self.txs length should never exceed [MAX_ADDRESSES]
                }
                virtual_time += 1;

                if queued.peek().is_some() {
                    Some((address, queued))
                } else {
                    None
                }
            })
            .collect::<IndexMap<_, _>>();

        let mut txs = Vec::new();
        let mut total_gas = 0u64;

        // loop invariant: `tx_heap` contains one transaction per account (lowest nonce).
        // the root of the heap will be the best priced eligible transaction
        while txs.len() < tx_limit {
            let Some(VirtualValidEthTransaction {
                tx: best_tx,
                virtual_time: _,
            }) = tx_heap.pop()
            else {
                break;
            };

            let sender = best_tx.sender();

            // If this transaction will take us out of the block limit, we cannot include it.
            // This will create a nonce gap, so we want to continue considering other accounts
            // without adding the address back to the `tx_heap`.
            if total_gas
                .checked_add(best_tx.gas_limit())
                .expect("total gas does not overflow")
                > proposal_gas_limit
            {
                continue;
            }

            // We also want to make sure that the transaction has max_fee_per_gas larger than base fee
            // TODO(kai): currently block base fee is hardcoded to 1000 in monad-ledger
            // update this when base fee is included in consensus proposal
            if best_tx.max_fee_per_gas() < 1000 {
                continue;
            }

            let account_balance = account_balances
                .get_mut(&sender)
                .expect("address in account_balances");

            match best_tx.apply_txn_fee(account_balance) {
                Ok(new_account_balance) => {
                    *account_balance = new_account_balance;

                    total_gas += best_tx.gas_limit();
                    trace!(txn_hash = ?best_tx.hash(), "txn included in proposal");
                    txs.push(best_tx.raw().to_owned());
                }
                Err(_) => {
                    continue;
                }
            }

            // Add a transaction back to `tx_heap` to maintain the loop invariant
            let Some(tx_iter) = tx_iters.get_mut(&sender) else {
                continue;
            };

            let Some(tx) = tx_iter.next() else {
                continue;
            };

            tx_heap
                .push(VirtualValidEthTransaction { tx, virtual_time })
                .expect("heap size never exceeds fixed capacity");
            virtual_time += 1;
        }

        Ok((total_gas, txs))
    }

    pub fn update_committed_block<SCT>(&mut self, committed_block: EthValidatedBlock<SCT>)
    where
        SCT: SignatureCollection,
    {
        if committed_block.is_empty_block() {
            // TODO this is error-prone, easy to forget
            // TODO write tests that fail if this doesn't exist
            return;
        }

        {
            let seqnum = committed_block.get_seq_num();
            debug!(?seqnum, "txpool updating committed block");
        }

        if self.last_commit_seq_num != SeqNum::MIN {
            assert_eq!(
                committed_block.get_seq_num(),
                self.last_commit_seq_num + SeqNum(1),
                "txpool received out of order committed block"
            );
        }
        self.last_commit_seq_num = committed_block.get_seq_num();

        for (address, highest_tx_nonce) in committed_block.nonces {
            match self.txs.entry(address) {
                IndexMapEntry::Vacant(_) => continue,
                IndexMapEntry::Occupied(tx_list) => TrackedTxList::update_account_nonce(
                    tx_list,
                    &highest_tx_nonce
                        .checked_add(1)
                        .expect("nonce does not overflow"),
                ),
            }
        }
    }

    pub fn clear(&mut self) {
        self.txs.clear();
    }

    pub(super) fn insert(
        &mut self,
        address: EthAddress,
        account_nonce: u64,
        pending: PendingTxList,
    ) {
        match self.txs.entry(address) {
            IndexMapEntry::Occupied(mut o) => {
                // TODO(andr-dev): Warnings?

                assert_eq!(
                    o.get().account_nonce(),
                    account_nonce,
                    "account nonces match"
                );

                for tx in pending.into_map().into_values() {
                    if let Err(_) = o.get_mut().try_add_tx(tx) {
                        // TODO(andr-dev): txpool metrics
                    }
                }
            }
            IndexMapEntry::Vacant(v) => {
                v.insert(TrackedTxList::new_from_account_nonce_and_pending(
                    account_nonce,
                    pending,
                ));
            }
        }
    }
}
