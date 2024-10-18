use alloy_rpc_client::{ClientBuilder, ReqwestClient, Waiter};
use clap::Parser;
use erc20::ERC20;
use eyre::{bail, Context, ContextCompat, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use generator::config::EthTxActivityType;
use reth_primitives::{Address, TransactionSigned, U256};
use state::ChainStateView;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::PathBuf,
    time::Duration,
};
use tokio::time::{sleep_until, Instant};
use tracing::{debug, error, info, trace, warn};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{
    fmt::{self, MakeWriter},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter, Layer,
};
use url::Url;

use crate::{
    generator::{EthTxGenerator, EthTxGeneratorConfig},
    state::ChainState,
};

mod account;
mod generator;
mod state;
mod erc20;

#[derive(Debug, Parser)]
#[command(name = "monad-node", about, long_about = None)]
pub struct EthTxGenCli {
    #[arg(long, default_value = "http://localhost:8080")]
    pub rpc_url: Url,

    #[arg(long)]
    pub config: PathBuf,
}

fn main() {
    setup_logging();

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
    trace!("Spawning chain state manager");
    let (mut chain_state_manager_handle, chain_state) =
        ChainState::new_with_manager(client.clone()).await;

    let (mut tx_generator, mut gen_timer) =
        EthTxGenerator::new(tx_generator_config, chain_state.clone()).await;

    trace!("Before checker");
    state::monitors::AccountChecker::spawn(
        tx_generator.account_pool.from.clone(),
        chain_state.clone(),
        client.clone(),
    );
    let mut batch_futs = FuturesUnordered::new();

    debug!("before main loop");

    let mut num_txs_sent = 0;
    let mut last_num_txs_sent = 0;
    let mut tx_sent_metric_timer = tokio::time::interval(Duration::from_secs(5));
    let mut last_time_sent = Instant::now();

    loop {
        tokio::select! {
            now = tx_sent_metric_timer.tick() => {
                let sent = num_txs_sent - last_num_txs_sent;
                let elapsed = last_time_sent.elapsed().as_secs_f64();
                last_num_txs_sent = num_txs_sent;
                info!(sent, secs_elapsed = elapsed, tps = sent as f64 / elapsed, "Sent tps");
                last_time_sent = now;
            }
            _ = gen_timer.tick() => {
                let tx_batch = tx_generator.generate().await;
                num_txs_sent += tx_batch.len();
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

fn setup_logging() {
    // trace log volume is *very* high, so we rotate and drop these aggressively
    let trace_layer = fmt::layer()
        .with_writer(
            RollingFileAppender::builder()
                .max_log_files(30)
                .filename_prefix("trace")
                .rotation(Rotation::MINUTELY)
                .build("./")
                .unwrap(),
        )
        .with_filter(EnvFilter::new("monad_eth_txgen=trace"));

    // retain debug logs for longer
    let debug_layer = fmt::layer()
        .with_writer(
            RollingFileAppender::builder()
                .max_log_files(5)
                .filename_prefix("debug")
                .rotation(Rotation::HOURLY)
                .build("./")
                .unwrap(),
        )
        .with_filter(EnvFilter::new("monad_eth_txgen=debug"));

    // log high signal aggregations to stdio
    let stdio_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_filter(EnvFilter::new("monad_eth_txgen=info"));

    // set up subscriber with all layers
    tracing_subscriber::registry()
        .with(trace_layer)
        .with(debug_layer)
        .with(stdio_layer)
        .init();
}
