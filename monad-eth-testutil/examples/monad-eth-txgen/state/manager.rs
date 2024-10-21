use std::{collections::HashSet, future::Future, str::FromStr, sync::Arc, time::Duration};

use alloy_json_rpc::RpcError;
use alloy_primitives::FixedBytes;
use alloy_rpc_client::ReqwestClient;
use alloy_transport::TransportErrorKind;
use eyre::{Context, ContextCompat};
use futures::{FutureExt, StreamExt};
use reth_primitives::{
    hex::{encode, ToHex},
    Address, TxHash, U256, U64,
};
use reth_rpc_types::{Block, BlockTransactions};
use ruint::Uint;
use thiserror::Error;
use tokio::{
    sync::RwLock,
    task::JoinHandle,
    time::{interval, sleep, Instant},
};
use tracing::{debug, error, info, trace, warn};

use crate::{
    erc20::{ERC20, IERC20},
    generator::format_addr,
    state::monitors::ChainMetrics,
};

use super::{
    blockstream::{BlockStream, BlockStreamError},
    monitors, ChainAccountState, ChainState, ChainStateView, SharedChainState,
};

pub struct ChainStateManager {
    client: ReqwestClient,
    blockstream: BlockStream,
    chain_state: SharedChainState,

    blocks_counter: usize,
    txs_counter: usize,
}

impl ChainStateManager {
    pub async fn new(client: ReqwestClient) -> (Self, ChainStateView) {
        let blockstream = BlockStream::new(client.clone(), Duration::from_millis(25)).await;

        let chain_state = Arc::new(RwLock::new(ChainState::default()));

        (
            Self {
                client,
                blockstream,
                chain_state: chain_state.clone(),
                blocks_counter: 0,
                txs_counter: 0,
            },
            ChainStateView::new(chain_state),
        )
    }

    pub fn start(self) -> ChainStateManagerHandle {
        trace!("start of chain state manager");
        let chain_state_view = ChainStateView::new(Arc::clone(&self.chain_state));

        // spawn monitors
        tokio::spawn(monitors::monitor_non_zero_accts(chain_state_view));
        trace!("spawn monitors done");

        let x = ChainStateManagerHandle(tokio::spawn(self.run()));
        trace!("spawned chain state manager");
        x
    }

    async fn run(mut self) -> Result<(), ChainStateManagerError> {
        info!("Chain state manager started");
        let mut fetch_new_accounts_timer = interval(Duration::from_millis(10));

        let mut chain_metrics = ChainMetrics::new();

        loop {
            tokio::select! {
                biased;

                now = chain_metrics.interval.tick() => {
                    chain_metrics.log(
                        &self.chain_state,
                        self.blocks_counter,
                        self.txs_counter,
                        now,
                    ).await;
                }

                result = self.blockstream.select_next_some() => {
                    debug!("Start Processing new block");
                    match result {
                        Ok(block) => if let Err(e) = self.process_new_block(block).await {
                            error!("Error processing new block: {e}");
                        },
                        Err(e) => error!("Blockstream returned error: {e}"),
                    }
                    debug!("End Processing new block");
                }

                _ = fetch_new_accounts_timer.tick() => {
                    if let Err(e) = self.fetch_new_accounts().await  {
                        error!("Fetching new accounts failed: {e}");
                    }
                }
            }
        }
    }

    async fn process_new_block(&mut self, block: Block) -> Result<(), ChainStateManagerError> {
        let txs = match block.transactions {
            BlockTransactions::Full(transactions) => transactions,
            BlockTransactions::Hashes(_) | BlockTransactions::Uncle => {
                if block.header.nonce != Some(FixedBytes::new([0u8; 8])) {
                    error!("got uncle for non-genesis block");
                }
                return Ok(());
            }
        };

        debug!(num_txs = txs.len(), "Processed block");
        self.blocks_counter += 1;
        self.txs_counter += txs.len();

        let mut touched_accts = HashSet::with_capacity(txs.len() * 2);
        let mut chain_state = self.chain_state.write().await;

        for tx in txs {
            if let Some(from_account_state) = chain_state.accounts.get_mut(&tx.from) {
                touched_accts.insert(tx.from);

                from_account_state.next_nonce = tx.nonce.to::<u64>() + 1;

                // TODO(abenedito): Verify balance computation
                let gas_cost: U256 =
                    tx.gas * U256::from(tx.gas_price.context("transaction has gas price")?);

                from_account_state.balance = from_account_state
                    .balance
                    .checked_sub(tx.value)
                    .wrap_err_with(|| {
                        format!("balance underflows value for account {:?}", tx.from)
                    })?
                    .checked_sub(gas_cost)
                    .context("balance does not underflow from gas cost")?;
            }

            if let Some(to_account_state) = tx.to.and_then(|to| chain_state.accounts.get_mut(&to)) {
                touched_accts.insert(tx.to.unwrap());
                to_account_state.balance = to_account_state.balance + tx.value;
            }
        }

        if let Some(erc20) = chain_state.erc20_to_check {
            let chain_state = self.chain_state.clone();
            let client = self.client.clone();
            tokio::spawn(async move {
                let mut iter = touched_accts.into_iter().peekable();

                while let Some(_) = iter.peek() {
                    let chunk = iter.by_ref().take(500).collect();
                    tokio::spawn(update_erc20_bals(
                        chain_state.clone(),
                        client.clone(),
                        erc20,
                        chunk,
                    ));
                    // don't hit rpc too hard
                    sleep(Duration::from_millis(50)).await;
                }
            });
        }

        return Ok(());
    }

    async fn fetch_new_accounts(&mut self) -> Result<(), ChainStateManagerError> {
        let Some(current_block_number) = self.blockstream.get_current_block_number() else {
            return Ok(());
        };

        let new_accounts = {
            let chain_state = self.chain_state.read().await;

            if chain_state.new_accounts.is_empty() {
                return Ok(());
            }

            const FETCH_LIMIT: usize = 500;

            debug!(
                total_new_accts = chain_state.new_accounts.len(),
                FETCH_LIMIT, "Fetching new accounts"
            );

            chain_state
                .new_accounts
                .iter()
                .take(FETCH_LIMIT)
                .cloned()
                .collect::<Vec<_>>()
        };

        let mut batch = self.client.new_batch();

        let calls = new_accounts
            .into_iter()
            .map(|address| {
                let params = [address.to_string(), format!("0x{current_block_number:x}")];

                let balance_fut = batch.add_call::<_, U256>("eth_getBalance", &params)?;
                let nonce_fut = batch.add_call::<_, U64>("eth_getTransactionCount", &params)?;

                Ok((address, balance_fut, nonce_fut))
            })
            .collect::<Result<Vec<_>, ChainStateManagerError>>()?;

        batch.send().await?;

        let mut chain_state = self.chain_state.write().await;

        for (address, balance_waiter, nonce_waiter) in calls {
            let balance = balance_waiter.await?;
            let nonce: u64 = nonce_waiter.await?.to();

            if !chain_state.new_accounts.remove(&address) {
                warn!("synced new account {address:?} when not requested!");
            }

            if let Some(existing) = chain_state
                .accounts
                .insert(address, ChainAccountState::new(balance, nonce))
            {
                warn!("added address {address:?} when already existing: {existing:#?}");
            }
        }

        debug!("Done fetch");
        Ok(())
    }
}

async fn update_erc20_bals(
    chain_state: SharedChainState,
    client: ReqwestClient,
    erc20: ERC20,
    touched_accts: Vec<Address>,
) {
    let mut batch = client.new_batch();
    let start = Instant::now();

    // add all eth_calls to batch
    let mut results = Vec::with_capacity(touched_accts.len());
    for addr in &touched_accts {
        let (method, params) = erc20.balance_of(*addr);
        let waiter = match batch.add_call::<_, U256>(method, &params) {
            Ok(waiter) => waiter,
            Err(_) => {
                warn!(
                    addr = addr.to_string(),
                    "Failed to add eth_call balanceOf(address) to batch"
                );
                continue;
            }
        };
        results.push(async move { (addr, waiter.await) });
    }

    // send the batched requests
    if let Err(e) = batch.send().await {
        error!("Fetch erc20 bals batch failed to send: {e}");
        return;
    }

    // resolve all futures before locking state
    let results = futures::future::join_all(results).await;

    let mut num_updated = 0;
    let mut chain_state = chain_state.write().await;
    for (addr, bal_result) in results {
        let bal = match bal_result {
            Ok(bal) => bal,
            Err(e) => {
                error!(
                    addr = addr.to_string(),
                    "Failed to fetch balanceOf for erc20: {e}"
                );
                continue;
            }
        };
        if let Some(acct) = chain_state.accounts.get_mut(addr) {
            num_updated += 1;
            acct.erc20_bal = bal;
        }
    }
    debug!(
        num_updated,
        total_touched = touched_accts.len(),
        elapsed_ms = start.elapsed().as_millis(),
        "Finished updating erc20 bals"
    );
}

#[derive(Debug, Error)]
pub enum ChainStateManagerError {
    #[error("TaskTerminated")]
    TaskTerminated,

    #[error(transparent)]
    RpcError(#[from] RpcError<TransportErrorKind>),

    #[error(transparent)]
    BlockStreamError(#[from] BlockStreamError),

    #[error("RpcResponseError(0)")]
    Expect(#[from] eyre::Error),
}

pub struct ChainStateManagerHandle(JoinHandle<Result<(), ChainStateManagerError>>);

impl Future for ChainStateManagerHandle {
    type Output = ChainStateManagerError;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.0.poll_unpin(cx) {
            std::task::Poll::Ready(Ok(Err(error))) => std::task::Poll::Ready(error),
            std::task::Poll::Ready(Ok(Ok(()))) | std::task::Poll::Ready(Err(_)) => {
                std::task::Poll::Ready(ChainStateManagerError::TaskTerminated)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}
