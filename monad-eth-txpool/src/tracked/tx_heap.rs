use std::collections::VecDeque;

use heapless::BinaryHeap;
use indexmap::IndexMap;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_eth_block_policy::{AccountNonceRetrievable, EthValidatedBlock};
use monad_eth_types::EthAddress;
use tracing::error;

use super::list::TrackedTxList;
use crate::transaction::ValidEthTransaction;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct OrderedTxGroup<'a> {
    tx: &'a ValidEthTransaction,
    virtual_time: u64,
    address: &'a EthAddress,
    queued: VecDeque<&'a ValidEthTransaction>,
}

pub struct TrackedTxHeap<'a, const MAX_ADDRESSES: usize> {
    heap: BinaryHeap<OrderedTxGroup<'a>, heapless::binary_heap::Max, MAX_ADDRESSES>,
    virtual_time: u64,
}

impl<'a, const MAX_ADDRESSES: usize> TrackedTxHeap<'a, MAX_ADDRESSES> {
    pub fn new<SCT>(
        tracked_txs: &'a IndexMap<EthAddress, TrackedTxList>,
        extending_blocks: &Vec<&EthValidatedBlock<SCT>>,
    ) -> Self
    where
        SCT: SignatureCollection,
    {
        let pending_account_nonces = extending_blocks.get_account_nonces();

        let mut this = Self {
            heap: BinaryHeap::new(),
            virtual_time: 0,
        };

        for (address, tx_list) in tracked_txs {
            let mut queued = tx_list.get_queued(pending_account_nonces.get(address).cloned());

            let Some(tx) = queued.next() else {
                continue;
            };

            this.push(address, tx, queued.collect::<VecDeque<_>>());
        }

        this
    }

    fn push(
        &mut self,
        address: &'a EthAddress,
        tx: &'a ValidEthTransaction,
        queued: VecDeque<&'a ValidEthTransaction>,
    ) {
        assert_eq!(address, &tx.sender());

        if let Err(_) = self.heap.push(OrderedTxGroup {
            tx,
            virtual_time: self.virtual_time,
            address,
            queued,
        }) {
            // TODO(andr-dev): Warn as self.txs length should never exceed [MAX_ADDRESSES]
            error!("txpool tracked heap invalid push");
        }

        self.virtual_time += 1;
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn addresses<'s>(&'s self) -> impl Iterator<Item = &'a EthAddress> + 's {
        self.heap.iter().map(
            |OrderedTxGroup {
                 tx: _,
                 virtual_time: _,
                 address,
                 queued: _,
             }| *address,
        )
    }

    pub fn drain_in_order_while(
        mut self,
        mut f: impl FnMut(&EthAddress, &ValidEthTransaction) -> TrackedTxHeapDrainAction,
    ) {
        while let Some(OrderedTxGroup {
            tx,
            virtual_time: _,
            address,
            mut queued,
        }) = self.heap.pop()
        {
            match f(address, tx) {
                TrackedTxHeapDrainAction::Skip => {}
                TrackedTxHeapDrainAction::Continue => {
                    if let Some(tx) = queued.pop_front() {
                        self.push(address, tx, queued);
                    }
                }
                TrackedTxHeapDrainAction::Stop => {
                    break;
                }
            }
        }
    }
}

pub enum TrackedTxHeapDrainAction {
    Skip,
    Continue,
    Stop,
}
