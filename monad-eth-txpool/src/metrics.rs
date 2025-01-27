use monad_executor::ExecutorMetrics;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TxPoolMetrics {
    pub insert_mempool_txs: u64,
    pub insert_forwarded_txs: u64,
    pub drop_invalid_bytes: u64,
    pub drop_not_well_formed: u64,
    pub drop_nonce_too_low: u64,
    pub drop_fee_too_low: u64,
    pub drop_insufficient_balance: u64,
    pub drop_pool_full: u64,
    pub drop_existing_higher_priority: u64,
    pub pending_addresses: u64,
    pub pending_txs: u64,
    pub pending_promote_addresses: u64,
    pub pending_promote_txs: u64,
    pub pending_drop_unknown_addresses: u64,
    pub pending_drop_unknown_txs: u64,
    pub pending_drop_low_nonce_addresses: u64,
    pub pending_drop_low_nonce_txs: u64,
    pub tracked_addresses: u64,
    pub tracked_txs: u64,
    pub tracked_evict_expired_addresses: u64,
    pub tracked_evict_expired_txs: u64,
    pub tracked_remove_committed_addresses: u64,
    pub tracked_remove_committed_txs: u64,
}
impl TxPoolMetrics {
    pub fn update(&self, metrics: &mut ExecutorMetrics) {
        metrics["monad.state.txpool_events.insert_mempool_txs"] = self.insert_mempool_txs;
        metrics["monad.state.txpool_events.insert_forwarded_txs"] = self.insert_forwarded_txs;
        metrics["monad.state.txpool_events.drop_invalid_bytes"] = self.drop_invalid_bytes;
        metrics["monad.state.txpool_events.drop_not_well_formed"] = self.drop_not_well_formed;
        metrics["monad.state.txpool_events.drop_nonce_too_low"] = self.drop_nonce_too_low;
        metrics["monad.state.txpool_events.drop_fee_too_low"] = self.drop_fee_too_low;
        metrics["monad.state.txpool_events.drop_insufficient_balance"] =
            self.drop_insufficient_balance;
        metrics["monad.state.txpool_events.drop_pool_full"] = self.drop_pool_full;
        metrics["monad.state.txpool_events.drop_existing_higher_priority"] =
            self.drop_existing_higher_priority;
        metrics["monad.state.txpool_events.pending_addresses"] = self.pending_addresses;
        metrics["monad.state.txpool_events.pending_txs"] = self.pending_txs;
        metrics["monad.state.txpool_events.pending_promote_addresses"] =
            self.pending_promote_addresses;
        metrics["monad.state.txpool_events.pending_promote_txs"] = self.pending_promote_txs;
        metrics["monad.state.txpool_events.pending_drop_unknown_addresses"] =
            self.pending_drop_unknown_addresses;
        metrics["monad.state.txpool_events.pending_drop_unknown_txs"] =
            self.pending_drop_unknown_txs;
        metrics["monad.state.txpool_events.pending_drop_low_nonce_addresses"] =
            self.pending_drop_low_nonce_addresses;
        metrics["monad.state.txpool_events.pending_drop_low_nonce_txs"] =
            self.pending_drop_low_nonce_txs;
        metrics["monad.state.txpool_events.tracked_addresses"] = self.tracked_addresses;
        metrics["monad.state.txpool_events.tracked_txs"] = self.tracked_txs;
        metrics["monad.state.txpool_events.tracked_evict_expired_addresses"] =
            self.tracked_evict_expired_addresses;
        metrics["monad.state.txpool_events.tracked_evict_expired_txs"] =
            self.tracked_evict_expired_txs;
        metrics["monad.state.txpool_events.tracked_remove_committed_addresses"] =
            self.tracked_remove_committed_addresses;
        metrics["monad.state.txpool_events.tracked_remove_committed_txs"] =
            self.tracked_remove_committed_txs;
    }
}
