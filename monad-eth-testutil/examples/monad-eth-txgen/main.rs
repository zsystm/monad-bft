use alloy_rpc_client::{ClientBuilder, ReqwestClient, Waiter};
use clap::Parser;
use eyre::{bail, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use reth_primitives::TransactionSigned;
use std::path::PathBuf;
use tokio::time::Instant;
use tracing::{debug, error, trace, warn};
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
    tracing_subscriber::fmt::init();
    let args = EthTxGenCli::parse();
    let config = EthTxGeneratorConfig::new_from_file(args.config).expect("Failed to load config");
    let client: ReqwestClient = ClientBuilder::default().http(args.rpc_url);

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
    let mut next_batch_time = Instant::now();
    let mut batch_futs = FuturesUnordered::new();

    loop {
        tokio::select! {
            () = tokio::time::sleep_until(next_batch_time) => {
                let (tx_batch, _next_batch_time) = tx_generator.generate().await;
                next_batch_time = _next_batch_time;

                send_batch(&client, &mut batch_futs, tx_batch);
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

fn send_batch(
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
