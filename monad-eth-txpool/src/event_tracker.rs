use std::{sync::atomic::Ordering, time::Instant};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_primitives::TxHash;
use monad_eth_txpool_types::{
    EthTxPoolDropReason, EthTxPoolEvent, EthTxPoolEvictReason, EthTxPoolInternalDropReason,
};

use crate::EthTxPoolMetrics;

pub struct EthTxPoolEventTracker<'a> {
    pub now: Instant,

    metrics: &'a EthTxPoolMetrics,
    events: &'a mut Vec<EthTxPoolEvent>,
}

impl<'a> EthTxPoolEventTracker<'a> {
    pub fn new(metrics: &'a EthTxPoolMetrics, events: &'a mut Vec<EthTxPoolEvent>) -> Self {
        Self {
            now: Instant::now(),

            metrics,
            events,
        }
    }

    pub fn insert_pending(&mut self, tx: &Recovered<TxEnvelope>, owned: bool) {
        if owned {
            self.metrics.insert_owned_txs.fetch_add(1, Ordering::SeqCst);
        } else {
            self.metrics
                .insert_forwarded_txs
                .fetch_add(1, Ordering::SeqCst);
        }

        self.events.push(EthTxPoolEvent::Insert {
            tx_hash: *tx.tx_hash(),
            address: tx.signer(),
            owned,
            tracked: false,
        });
    }

    pub fn insert_tracked(&mut self, tx: &Recovered<TxEnvelope>, owned: bool) {
        if owned {
            self.metrics.insert_owned_txs.fetch_add(1, Ordering::SeqCst);
        } else {
            self.metrics
                .insert_forwarded_txs
                .fetch_add(1, Ordering::SeqCst);
        }

        self.events.push(EthTxPoolEvent::Insert {
            tx_hash: *tx.tx_hash(),
            address: tx.signer(),
            owned,
            tracked: true,
        });
    }

    pub fn replace_pending(&mut self, old_tx_hash: TxHash, new_tx_hash: TxHash, new_owned: bool) {
        if new_owned {
            self.metrics.insert_owned_txs.fetch_add(1, Ordering::SeqCst);
        } else {
            self.metrics
                .insert_forwarded_txs
                .fetch_add(1, Ordering::SeqCst);
        }

        self.events.push(EthTxPoolEvent::Replace {
            old_tx_hash,
            new_tx_hash,
            new_owned,
            tracked: false,
        });
    }

    pub fn replace_tracked(&mut self, old_tx_hash: TxHash, new_tx_hash: TxHash, new_owned: bool) {
        if new_owned {
            self.metrics.insert_owned_txs.fetch_add(1, Ordering::SeqCst);
        } else {
            self.metrics
                .insert_forwarded_txs
                .fetch_add(1, Ordering::SeqCst);
        }

        self.events.push(EthTxPoolEvent::Replace {
            old_tx_hash,
            new_tx_hash,
            new_owned,
            tracked: true,
        });
    }

    pub fn drop(&mut self, tx_hash: TxHash, reason: EthTxPoolDropReason) {
        match reason {
            EthTxPoolDropReason::NotWellFormed(_) => {
                self.metrics
                    .drop_not_well_formed
                    .fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::InvalidSignature => {
                self.metrics
                    .drop_invalid_signature
                    .fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::NonceTooLow => {
                self.metrics
                    .drop_nonce_too_low
                    .fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::FeeTooLow => {
                self.metrics.drop_fee_too_low.fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::InsufficientBalance => {
                self.metrics
                    .drop_insufficient_balance
                    .fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::ExistingHigherPriority => {
                self.metrics
                    .drop_existing_higher_priority
                    .fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::PoolFull => {
                self.metrics.drop_pool_full.fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::PoolNotReady => {
                self.metrics
                    .drop_pool_not_ready
                    .fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::Internal(EthTxPoolInternalDropReason::StateBackendError) => {
                self.metrics
                    .drop_internal_state_backend_error
                    .fetch_add(1, Ordering::SeqCst);
            }
        }

        self.events.push(EthTxPoolEvent::Drop { tx_hash, reason });
    }

    pub fn drop_all(
        &mut self,
        txs: impl Iterator<Item = Recovered<TxEnvelope>>,
        reason: EthTxPoolDropReason,
    ) {
        for tx in txs {
            self.drop(tx.tx_hash().to_owned(), reason);
        }
    }

    pub fn pending_promote(&mut self, tx_hashes: impl Iterator<Item = TxHash>) {
        self.metrics
            .pending
            .promote_addresses
            .fetch_add(1, Ordering::SeqCst);

        for tx_hash in tx_hashes {
            self.metrics
                .pending
                .promote_txs
                .fetch_add(1, Ordering::SeqCst);

            self.events.push(EthTxPoolEvent::Promoted {
                tx_hash: tx_hash.to_owned(),
            });
        }
    }

    pub fn pending_drop_unknown(&mut self, tx_hashes: impl Iterator<Item = TxHash>) {
        self.metrics
            .pending
            .drop_unknown_addresses
            .fetch_add(1, Ordering::SeqCst);

        for tx_hash in tx_hashes {
            self.metrics
                .pending
                .drop_unknown_txs
                .fetch_add(1, Ordering::SeqCst);

            self.events.push(EthTxPoolEvent::Drop {
                tx_hash,
                reason: EthTxPoolDropReason::InsufficientBalance,
            });
        }
    }

    pub fn pending_drop_low_nonce(
        &mut self,
        address: bool,
        tx_hashes: impl Iterator<Item = TxHash>,
    ) {
        if address {
            self.metrics
                .pending
                .drop_low_nonce_addresses
                .fetch_add(1, Ordering::SeqCst);
        }

        for tx_hash in tx_hashes {
            self.metrics
                .pending
                .drop_low_nonce_txs
                .fetch_add(1, Ordering::SeqCst);

            self.events.push(EthTxPoolEvent::Drop {
                tx_hash,
                reason: EthTxPoolDropReason::NonceTooLow,
            });
        }
    }

    pub fn tracked_commit(&mut self, address: bool, tx_hashes: impl Iterator<Item = TxHash>) {
        if address {
            self.metrics
                .tracked
                .remove_committed_addresses
                .fetch_add(1, Ordering::SeqCst);
        }

        for tx_hash in tx_hashes {
            self.metrics
                .tracked
                .remove_committed_txs
                .fetch_add(1, Ordering::SeqCst);

            self.events.push(EthTxPoolEvent::Commit { tx_hash });
        }
    }

    pub fn tracked_evict_expired(
        &mut self,
        address: bool,
        tx_hashes: impl Iterator<Item = TxHash>,
    ) {
        if address {
            self.metrics
                .tracked
                .evict_expired_addresses
                .fetch_add(1, Ordering::SeqCst);
        }

        for tx_hash in tx_hashes {
            self.metrics
                .tracked
                .evict_expired_txs
                .fetch_add(1, Ordering::SeqCst);

            self.events.push(EthTxPoolEvent::Evict {
                tx_hash,
                reason: EthTxPoolEvictReason::Expired,
            });
        }
    }

    pub fn update_aggregate_metrics(
        &mut self,
        pending_addresses: u64,
        pending_txs: u64,
        tracked_addresses: u64,
        tracked_txs: u64,
    ) {
        self.metrics
            .pending
            .addresses
            .store(pending_addresses, Ordering::SeqCst);
        self.metrics
            .pending
            .txs
            .store(pending_txs, Ordering::SeqCst);
        self.metrics
            .tracked
            .addresses
            .store(tracked_addresses, Ordering::SeqCst);
        self.metrics
            .tracked
            .txs
            .store(tracked_txs, Ordering::SeqCst);
    }

    pub fn record_create_proposal(
        &mut self,
        tracked_addresses: usize,
        available_addresses: usize,
        backend_lookups: u64,
        proposal_txs: usize,
    ) {
        self.metrics.create_proposal.fetch_add(1, Ordering::SeqCst);
        self.metrics
            .create_proposal_txs
            .fetch_add(proposal_txs as u64, Ordering::SeqCst);
        self.metrics
            .create_proposal_tracked_addresses
            .fetch_add(tracked_addresses as u64, Ordering::SeqCst);
        self.metrics
            .create_proposal_available_addresses
            .fetch_add(available_addresses as u64, Ordering::SeqCst);
        self.metrics
            .create_proposal_backend_lookups
            .fetch_add(backend_lookups, Ordering::SeqCst);
    }
}
