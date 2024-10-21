use std::{
    iter,
    sync::atomic::{AtomicUsize, Ordering::SeqCst},
};

use futures::{stream::FuturesUnordered, StreamExt};
use monad_consensus_types::metrics;
use rand::rngs::SmallRng;
use reth_primitives::TransactionSigned;
use serde::Deserialize;

use crate::{
    prelude::*,
    shared::{erc20::ERC20, json_rpc::JsonRpc},
};

pub mod gen;
pub mod refresher;
pub mod rpc_sender;

pub use gen::*;
pub use refresher::*;
pub use rpc_sender::*;

pub const BATCH_SIZE: usize = 500;

#[derive(Clone, Default)]
pub struct SimpleAccount {
    pub nonce: u64,
    pub native_bal: U256,
    pub erc20_bal: U256,
    pub key: PrivateKey,
    pub addr: Address,
}

pub type Accounts = Vec<SimpleAccount>;

pub struct AccountsWithTime {
    accts: Accounts,
    sent: Instant,
}

pub struct AccountsWithTxs {
    accts: Accounts,
    txs: Vec<Vec<TransactionSigned>>,
    to_accts: Vec<Accounts>,
}

#[derive(Default)]
pub struct Metrics {
    pub accts_with_nonzero_bal: AtomicUsize,
    pub total_txs_sent: AtomicUsize,
    pub total_rpc_calls: AtomicUsize,
}

pub struct RecipientTracker {
    pub client: ReqwestClient,
    pub rpc_sender_rx: mpsc::Receiver<AccountsWithTime>,
    pub erc20: ERC20,
    pub delay: Duration,

    pub metrics: Arc<Metrics>,
}
impl RecipientTracker {
    pub async fn run(mut self) {
        let mut fetch_interval = tokio::time::interval(Duration::from_millis(10));
        fetch_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        while let Some(AccountsWithTime { mut accts, sent }) = self.rpc_sender_rx.recv().await {
            debug!(
                num_accts = accts.len(),
                channel_len = self.rpc_sender_rx.len(),
                "Recipient tracker received accts from rpc sender"
            );

            if sent + self.delay >= Instant::now() {
                tokio::time::sleep_until(sent + self.delay).await;
                debug!(
                    num_recipients = accts.len(),
                    "Recipient tracker waited delay, refreshing batch..."
                );
            }

            for batch in accts.chunks_mut(BATCH_SIZE) {
                if batch.is_empty() {
                    break;
                }

                let addr_strings = batch.iter().map(|a| a.addr.to_string()).collect::<Vec<_>>();

                self.handle_batch(addr_strings);

                fetch_interval.tick().await;
            }
        }
    }

    fn handle_batch(&self, addr_strings: Vec<String>) {
        let client = self.client.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let now = Instant::now();
            trace!("before recipient refresh");

            let bals = match client.batch_get_balance(&addr_strings).await {
                Ok(bals) => bals,
                Err(e) => {
                    warn!("Recipient tracker failed to refresh batch: {e}");
                    return;
                }
            };

            trace!(
                elapsed_ms = now.elapsed().as_millis(),
                "after recipient refresh"
            );

            let non_zero = bals
                .into_iter()
                .filter_map(Result::ok)
                .filter(|b| !b.is_zero())
                .count();

            metrics.accts_with_nonzero_bal.fetch_add(non_zero, SeqCst);
        });
    }
}

impl Metrics {
    pub async fn run(self: Arc<Metrics>) {
        let mut report_interval = tokio::time::interval(Duration::from_secs(2));
        report_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut prev_non_zero = self.accts_with_nonzero_bal.load(SeqCst);
        let mut prev_txs_sent = self.total_txs_sent.load(SeqCst);
        let mut prev_rpc_calls = self.total_rpc_calls.load(SeqCst);
        let mut last = Instant::now();

        loop {
            let now = report_interval.tick().await;

            let total_nonzero_accts = self.accts_with_nonzero_bal.load(SeqCst);
            let total_txs_sent = self.total_txs_sent.load(SeqCst);
            let total_rpc_call = self.total_rpc_calls.load(SeqCst);

            let elapsed = last.elapsed().as_secs_f64();

            let accts_created_per_sec =
                ((total_nonzero_accts - prev_non_zero) as f64 / elapsed).round() as u32;
            let tps = ((total_txs_sent - prev_txs_sent) as f64 / elapsed).round() as u32;
            let rps = ((total_rpc_call - prev_rpc_calls) as f64 / elapsed).round() as u32;

            info!(
                total_nonzero_accts,
                total_rpc_call, total_txs_sent, rps, accts_created_per_sec, tps, "Metrics"
            );

            prev_non_zero = total_nonzero_accts;
            prev_rpc_calls = total_rpc_call;
            prev_txs_sent = total_txs_sent;
            last = now;
        }
    }
}

impl std::fmt::Display for SimpleAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Account{{ nonce: {}, native: {:+.2e}, erc20: {:+.2e}, addr: {}}}",
            self.nonce,
            self.native_bal.to::<u128>(),
            self.erc20_bal.to::<u128>(),
            self.addr,
        )
    }
}
