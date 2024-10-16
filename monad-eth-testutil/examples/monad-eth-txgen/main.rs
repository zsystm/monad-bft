use alloy_rpc_client::{ClientBuilder, ReqwestClient, Waiter};
use clap::Parser;
use eyre::{bail, Context, ContextCompat, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use reth_primitives::{Address, TransactionSigned, U256};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::PathBuf,
    time::Duration,
};
use tokio::time::{sleep_until, Instant};
use tracing::{debug, error, info, trace, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};
use url::Url;

use crate::{
    generator::{EthTxGenerator, EthTxGeneratorConfig},
    state::ChainState,
};

mod account;
mod generator;
mod state;

#[derive(Debug, Parser)]
#[command(name = "monad-node", about, long_about = None)]
pub struct EthTxGenCli {
    #[arg(long, default_value = "http://localhost:8080")]
    pub rpc_url: Url,

    #[arg(long)]
    pub config: PathBuf,
}

fn main() {
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(File::create("logs.txt").unwrap())
                .with_filter(EnvFilter::new("monad_eth_txgen=trace")),
        )
        .with(
            fmt::layer()
                .with_writer(std::io::stdout)
                .with_filter(EnvFilter::new("monad_eth_txgen=info")),
        )
        .init();

    let args = EthTxGenCli::parse();
    let config = EthTxGeneratorConfig::new_from_file(args.config).expect("Failed to load config");
    let client: ReqwestClient = ClientBuilder::default().http(args.rpc_url);

    info!("Config: {config:?}");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    if let Err(e) = runtime.block_on(run(client, config)) {
        error!("txgen crashed on error: {e}");
        std::process::exit(1);
    }
}

async fn run(client: ReqwestClient, tx_generator_config: EthTxGeneratorConfig) -> Result<()> {
    let (mut chain_state_manager_handle, chain_state) =
        ChainState::new_with_manager(client.clone()).await;

    let mut tx_generator = EthTxGenerator::new(tx_generator_config, chain_state.clone()).await;
    let mut batch_futs = FuturesUnordered::new();
    let mut next_batch_time = Instant::now();

    AccountChecker::spawn(
        tx_generator.account_pool.from.clone(),
        chain_state.clone(),
        client.clone(),
    );

    loop {
        tokio::select! {
            _ = sleep_until(next_batch_time) => {
                let (tx_batch, instant) = tx_generator.generate().await;
                next_batch_time = instant;
                send_batch(&client, &mut batch_futs, tx_batch).await;
            }
            result = batch_futs.select_next_some(), if !batch_futs.is_empty() => {
                if let Err(err) = result {
                    warn!("expected tx fut to resolve to valid tx hash, e: {}", err);
                }
                // do smt with tx_hash
            }
            error = &mut chain_state_manager_handle => {
                bail!(error);
            }
        }
    }
}

pub struct AccountChecker {
    addrs: Vec<Address>,
    chain_state: state::ChainStateView,
    client: ReqwestClient,
    period: Duration,

    untracked: HashSet<Address>,
    pending_wrong: HashMap<Address, (u64, u64)>,
    confirmed_wrong: HashMap<Address, (u64, u64)>,
}

impl AccountChecker {
    pub fn spawn(addrs: Vec<Address>, chain_state: state::ChainStateView, client: ReqwestClient) {
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
        tokio::spawn(checker.run());
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

            let mut non_zero_balances = 0;
            let mut chain_state_non_zero_balances = 0;

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

                if balance != U256::ZERO {
                    non_zero_balances += 1;
                }
                if managers_view.get_balance() != U256::ZERO {
                    chain_state_non_zero_balances += 1;
                }

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
                    num_addrs = self.confirmed_wrong.len(),
                    "Account checker found accounts where manager's view of nonce is transiently old"
                )
            }

            info!(
                non_zero_balances,
                chain_state_non_zero_balances, "Accounts with nonzero balances"
            );

            if self.untracked.len() > 0 {
                info!(
                    num_untracked = self.untracked.len(),
                    "Manager is not tracking all 'from' accounts"
                );
            }
        }
    }
}

async fn send_batch(
    client: &ReqwestClient,
    batch_futs: &mut FuturesUnordered<Waiter<String>>,
    tx_batch: Vec<TransactionSigned>,
) {
    if tx_batch.is_empty() {
        return;
    }

    let mut batch_req = client.new_batch();

    let num_txs = tx_batch.len();
    for tx in tx_batch {
        match batch_req.add_call("eth_sendRawTransaction", &[tx.envelope_encoded()]) {
            Ok(fut) => batch_futs.push(fut),
            Err(e) => error!("Error adding call to batch_req: {}", e),
        }
    }

    let req_fut = batch_req.send();
    tokio::spawn(async move {
        debug!(num_txs, "Sending txs");
        if let Err(e) = req_fut.await {
            error!("Failed to send batch req: {}", e);
        }
        trace!(num_txs, "Txs sent");
    });
}
