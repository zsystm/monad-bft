use std::{collections::VecDeque, time::Duration};

use eyre::Result;
use reth_primitives::{Signature, Transaction};
use tokio::time::Instant;
use tracing::{trace, warn};

use crate::{account::PrivateKey, state::ChainAccountState};

#[derive(Debug)]
pub struct EthAccount {
    key: PrivateKey,
    next_nonce: u64,
    pending_tx_timestamp_retries: VecDeque<(Instant, usize)>,
}

impl std::fmt::Display for EthAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EthAccount{{ nonce: {}, pending:", self.next_nonce)?;
        for (t, nonce) in &self.pending_tx_timestamp_retries {
            write!(f, " {}: {},", nonce, t.elapsed().as_millis())?;
        }
        write!(f, "}}")
    }
}

impl EthAccount {
    pub fn new(key: PrivateKey) -> Self {
        Self {
            key,
            next_nonce: 0,
            pending_tx_timestamp_retries: VecDeque::default(),
        }
    }

    pub fn key(&self) -> &PrivateKey {
        &self.key
    }

    pub fn sign_transaction(&self, transaction: &Transaction) -> Result<Signature> {
        self.key.sign_transaction(transaction)
    }

    pub fn get_available_nonce(
        &mut self,
        account_state: &ChainAccountState,
        max_inflight: usize,
        trace: bool,
    ) -> Option<u64> {
        self.update_nonce(account_state, trace);

        for nonce_idx in 0..max_inflight {
            if let Some(nonce) = self.get_available_nonce_at_idx(nonce_idx) {
                return Some(nonce);
            }
        }

        None
    }

    fn update_nonce(&mut self, account_state: &ChainAccountState, trace: bool) {
        let nonce = account_state.get_next_nonce();
        if trace {
            trace!(
                chain_state_nonce = nonce,
                generator_nonce = self.next_nonce,
                "update nonce"
            );
        }

        while self.next_nonce < nonce {
            self.next_nonce = self
                .next_nonce
                .checked_add(1)
                .expect("last nonce does not overflow");

            if self.pending_tx_timestamp_retries.pop_front().is_none() {
                break;
            }
        }

        assert!((nonce == self.next_nonce) || self.pending_tx_timestamp_retries.is_empty());

        self.next_nonce = nonce;
    }

    fn get_available_nonce_at_idx(&mut self, nonce_idx: usize) -> Option<u64> {
        let tx_nonce = self
            .next_nonce
            .checked_add(nonce_idx.try_into().expect("nonce idx is valid u64"))
            .expect("tx nonce does not overflow");

        let Some((existing_tx_timesstamp, existing_tx_retries)) =
            self.pending_tx_timestamp_retries.get_mut(nonce_idx)
        else {
            assert_eq!(self.pending_tx_timestamp_retries.len(), nonce_idx);

            self.pending_tx_timestamp_retries
                .push_back((Instant::now(), 0));

            return Some(tx_nonce);
        };

        let now = Instant::now();

        if now
            < existing_tx_timesstamp
                .checked_add(Duration::from_secs(1))
                .expect("now does not overflow")
        {
            return None;
        }

        *existing_tx_timesstamp = now;
        *existing_tx_retries += 1;

        if *existing_tx_retries >= 5 {
            warn!("retried nonce at least five times :(");
        }

        // clear later nonces
        trace!("Truncating nonces");
        self.pending_tx_timestamp_retries.truncate(nonce_idx);

        Some(tx_nonce)
    }
}
