use std::{future::Future, sync::Arc, time::Duration};

use alloy_json_rpc::RpcError;
use alloy_primitives::FixedBytes;
use alloy_rpc_client::ReqwestClient;
use alloy_transport::TransportErrorKind;
use eyre::{Context, ContextCompat};
use futures::{FutureExt, StreamExt};
use reth_primitives::U256;
use reth_rpc_types::{Block, BlockTransactions};
use ruint::Uint;
use thiserror::Error;
use tokio::{
    sync::RwLock,
    task::JoinHandle,
    time::{interval, Instant},
};
use tracing::{debug, error, info, trace, warn};

use crate::generator::format_addr;

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
        // spawn monitors
        trace!("start of chain state manager");
        let chain_state_view = ChainStateView::new(Arc::clone(&self.chain_state));
        tokio::spawn(monitors::monitor_non_zero_accts(chain_state_view));
        trace!("spawn monitors done");

        let x = ChainStateManagerHandle(tokio::spawn(self.run()));
        trace!("spawned chain state manager");
        x
    }

    async fn run(mut self) -> Result<(), ChainStateManagerError> {
        info!("Chain state manager started");
        let mut fetch_new_accounts_timer = interval(Duration::from_millis(10));

        let mut last_blocks_count = 0;
        let mut last_txs_count = 0;
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        let mut last_time = Instant::now();

        loop {
            tokio::select! {
                biased;

                now = interval.tick() => {
                    let new_blocks = self.blocks_counter - last_blocks_count;
                    let elapsed = (now - last_time).as_millis();
                    let new_txs = self.txs_counter - last_txs_count;
                    let block_time = elapsed / new_blocks.max(1) as u128;
                    let tps = new_txs as u128 * 1000 / elapsed.max(1);

                    last_blocks_count = self.blocks_counter;
                    last_txs_count = self.txs_counter;
                    last_time = now;
                    let seeded_accounts = self.chain_state
                        .read()
                        .await
                        .accounts
                        .iter()
                        .filter(|a| a.1.balance.gt(&Uint::from(0)))
                        .count();


                    info!(elapsed, new_blocks, new_txs, block_time, tps, seeded_accounts, "Chain metrics");
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
        let transactions = match block.transactions {
            BlockTransactions::Full(transactions) => transactions,
            BlockTransactions::Hashes(_) | BlockTransactions::Uncle => {
                if block.header.nonce != Some(FixedBytes::new([0u8; 8])) {
                    error!("got uncle for non-genesis block");
                }
                return Ok(());
            }
        };

        let mut chain_state = self.chain_state.write().await;

        debug!(num_txs = transactions.len(), "Processed block");
        self.blocks_counter += 1;
        self.txs_counter += transactions.len();

        for transaction in transactions {
            if let Some(from_account_state) = chain_state.accounts.get_mut(&transaction.from) {
                let [nonce] = transaction.nonce.into_limbs();

                // TODO(abenedito): Verify balance computation
                let gas_cost = transaction
                    .gas
                    .checked_mul(Uint::<256, 4>::from_limbs_slice(
                        &transaction
                            .gas_price
                            .context("transaction has gas price")?
                            .into_limbs(),
                    ))
                    .context("gas cost does not overflow")?;

                from_account_state.balance = from_account_state
                    .balance
                    .checked_sub(transaction.value)
                    .wrap_err_with(|| {
                        format!(
                            "balance underflows value for account {:?}",
                            transaction.from
                        )
                    })?
                    .checked_sub(gas_cost)
                    .context("balance does not underflow from gas cost")?;

                from_account_state.next_nonce =
                    nonce.checked_add(1).context("nonce does not overflow")?;
            }

            if let Some(to_account_state) = transaction
                .to
                .and_then(|to| chain_state.accounts.get_mut(&to))
            {
                to_account_state.balance = to_account_state
                    .balance
                    .checked_add(transaction.value)
                    .context("balanced does not overflow")?
            }
        }

        return Ok(());
    }

    async fn fetch_new_accounts(&mut self) -> Result<(), ChainStateManagerError> {
        trace!("Start fetch");
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

                let balance_fut = batch.add_call("eth_getBalance", &params)?;
                let nonce_fut = batch.add_call("eth_getTransactionCount", &params)?;

                Ok((address, balance_fut, nonce_fut))
            })
            .collect::<Result<Vec<_>, ChainStateManagerError>>()?;

        batch.send().await?;

        let mut chain_state = self.chain_state.write().await;

        let mut count_not_inserted = 0;
        for (address, balance_waiter, nonce_waiter) in calls {
            trace!(addr = format_addr(&address), "Inserting new account");

            let balance = {
                let balance_str: String = balance_waiter.await?;
                let balance_str = balance_str.strip_prefix("0x").unwrap_or(&balance_str);
                U256::from_str_radix(&balance_str, 16).context("balance string is valid U256")?
            };

            let nonce_str: String = nonce_waiter.await?;
            let nonce = u64::from_str_radix(
                nonce_str
                    .strip_prefix("0x")
                    .context("nonce string always has 0x prefix")?,
                16,
            )
            .context("nonce string is valid u64")?;

            trace!(
                addr = format_addr(&address),
                "Removing from new_accounts set"
            );
            if !chain_state.new_accounts.remove(&address) {
                warn!("synced new account {address:?} when not requested!");
            }

            trace!(addr = format_addr(&address), "Inserting new account");
            if let Some(existing) = chain_state
                .accounts
                .insert(address, ChainAccountState::new(balance, nonce))
            {
                warn!("added address {address:?} when already existing: {existing:#?}");
            }
        }

        trace!("Done fetch");
        Ok(())
    }
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
