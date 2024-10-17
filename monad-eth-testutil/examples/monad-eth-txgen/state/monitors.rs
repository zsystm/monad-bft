use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use alloy_rpc_client::{ReqwestClient, Waiter};
use eyre::{Context, ContextCompat};
use reth_primitives::{Address, U256};
use tokio::time::{sleep_until, Instant};
use tracing::{error, info, trace, warn};

use super::ChainStateView;

pub async fn monitor_non_zero_accts(chain_state: ChainStateView) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));

    loop {
        interval.tick().await;

        let num_non_zero_accts = {
            let chain_state = chain_state.read().await;

            chain_state
                .accounts
                .iter()
                .filter(|a| a.1.get_balance() > U256::ZERO)
                .count()
        };

        info!(
            num_non_zero_accts,
            "Total number of accounts with non-zero balance"
        );
    }
}

pub struct AccountChecker {
    addrs: Vec<Address>,
    chain_state: ChainStateView,
    client: ReqwestClient,
    period: Duration,

    untracked: HashSet<Address>,
    pending_wrong: HashMap<Address, (u64, u64)>,
    confirmed_wrong: HashMap<Address, (u64, u64)>,
}

impl AccountChecker {
    pub fn spawn(addrs: Vec<Address>, chain_state: ChainStateView, client: ReqwestClient) {
        let pending_wrong = HashMap::with_capacity(addrs.len());
        let confirmed_wrong = HashMap::with_capacity(addrs.len());
        let untracked = HashSet::with_capacity(addrs.len());
        let period = Duration::from_secs(5);
        let checker = AccountChecker {
            addrs,
            chain_state,
            client,
            period,
            pending_wrong,
            confirmed_wrong,
            untracked,
        };
        trace!("before spawned account checker");
        tokio::spawn(checker.run());
        trace!("spawned account checker, returning");
    }

    async fn run(mut self) {
        let mut calls = Vec::with_capacity(self.addrs.len());
        let mut last = Instant::now();

        loop {
            sleep_until(last + self.period).await;
            last = Instant::now();

            // query nonces for each account in batches
            for addrs in self.addrs.chunks(500) {
                let mut batch_req = self.client.new_batch();
                for addr in addrs.iter().cloned() {
                    let nonce_fut = match batch_req.add_call(
                        "eth_getTransactionCount",
                        &[
                            &addr.to_string(), //
                            "latest",
                        ],
                    ) {
                        Ok(fut) => fut,
                        Err(e) => {
                            error!("Error adding call to batch_req for addr {addr}: {e}");
                            continue;
                        }
                    };
                    let bal_fut = match batch_req.add_call(
                        "eth_getBalance",
                        &[
                            &addr.to_string(), //
                            "latest",
                        ],
                    ) {
                        Ok(fut) => fut,
                        Err(e) => {
                            error!("Error adding call to batch_req for addr {addr}: {e}");
                            continue;
                        }
                    };

                    // what happens to nocne_fut if the batch send fails?
                    calls.push((addr, nonce_fut, bal_fut));
                }
                if let Err(e) = batch_req.send().await {
                    error!("Failed sending account checker rpc batch: {e}");
                }
            }

            let parse_balance = |w: Waiter<String>| async {
                let balance_str: String = w.await?;
                let balance_str = balance_str
                    .strip_prefix("0x")
                    .context("balance always has a 0x prefix")?;
                U256::from_str_radix(&balance_str, 16).context("balance string is valid U256")
            };
            let parse_nonce = |w: Waiter<String>| async {
                let s = w.await?;
                u64::from_str_radix(
                    s.strip_prefix("0x")
                        .context("nonce string always has 0x prefix")?,
                    16,
                )
                .context("nonce string is valid u64")
            };

            for (address, nonce_waiter, bal_waiter) in calls.drain(0..calls.len()) {
                let balance = match parse_balance(bal_waiter).await {
                    Ok(n) => n,
                    Err(e) => {
                        error!("Error parsing balance in checker: {e}");
                        continue;
                    }
                };

                let nonce = match parse_nonce(nonce_waiter).await {
                    Ok(n) => n,
                    Err(e) => {
                        error!("Error parsing nonce in checker: {e}");
                        continue;
                    }
                };

                let chain_state = self.chain_state.read().await;
                let Some(managers_view) = chain_state.get_account(&address) else {
                    self.untracked.insert(address);
                    continue;
                };

                if managers_view.get_next_nonce() != nonce {
                    let entry = self.pending_wrong.get(&address);

                    // if pending has entry and manager has not passed old entry:
                    if let Some((old_queried, _old_managers_view)) = entry {
                        trace!(
                            old_queried,
                            _old_managers_view,
                            nonce,
                            address = address.to_string(),
                            "in pending"
                        );
                        if managers_view.get_next_nonce() < *old_queried {
                            self.pending_wrong.remove(&address);
                            self.confirmed_wrong
                                .insert(address, (nonce, managers_view.get_next_nonce()));
                        } else {
                            // if manager has caught up to old entry, reupdate pending
                            self.pending_wrong
                                .insert(address, (nonce, managers_view.get_next_nonce()));
                            continue;
                        }
                    } else {
                        // else this is a new inconsistency
                        self.pending_wrong
                            .insert(address, (nonce, managers_view.get_next_nonce()));
                    }
                } else {
                    self.pending_wrong.remove(&address);
                    self.confirmed_wrong.remove(&address);
                }
            }

            for (address, (observed, managers_view)) in self.confirmed_wrong.iter() {
                trace!(
                    address = address.to_string(),
                    observed,
                    managers_view,
                    "Found account where state manager has incorrect view"
                );
            }
            if self.confirmed_wrong.len() > 0 {
                warn!(
                    num_addrs = self.confirmed_wrong.len(),
                    "Account checker found accounts where manager's view of nonce is consistently old"
                );
            } else if self.pending_wrong.len() > 0 {
                info!(
                    num_addrs = self.pending_wrong.len(),
                    "Account checker found accounts where manager's view of nonce is transiently old"
                )
            }

            if self.untracked.len() > 0 {
                info!(
                    num_untracked = self.untracked.len(),
                    "Manager is not tracking all 'from' accounts"
                );
            }
            self.untracked.clear();
        }
    }
}
