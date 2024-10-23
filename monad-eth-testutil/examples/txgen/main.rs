#![allow(async_fn_in_trait)]

use alloy_rpc_client::ClientBuilder;
use clap::Parser;
use prelude::*;
use tracing_subscriber::util::SubscriberInitExt;
use url::Url;
use workers::TxType;

// pub mod complex;
pub mod prelude;
pub mod run;
pub mod shared;
pub mod workers;

#[derive(Debug, Parser)]
#[command(name = "monad-node", about, long_about = None)]
pub struct Config {
    #[arg(long, default_value = "http://localhost:8080")]
    pub rpc_url: Url,

    #[arg(long, default_value = "1000")]
    pub tps: u64,

    #[arg(
        long,
        default_value = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
    )]
    pub root_private_key: String,

    #[arg(long, default_value = "10101")]
    pub recipient_seed: u64,

    #[arg(long, default_value = "10101")]
    pub sender_seed: u64,

    #[arg(long, default_value = "native")]
    pub tx_type: TxType,

    #[arg(long, default_value = "100000")]
    pub recipients: usize,

    #[arg(long, default_value = "1000")]
    pub senders: usize,

    #[arg(long, default_value = "50")]
    pub sender_group_size: usize,

    #[arg(long, default_value = "5.")]
    pub refresh_delay_secs: f64,

    #[arg(long, default_value = "false")]
    pub erc20_balance_of: bool,

    #[arg(long, default_value = "500")]
    pub tx_batch_size: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    setup_logging()?;

    let config = Config::parse();
    let client: ReqwestClient = ClientBuilder::default().http(config.rpc_url.clone());

    info!("Config: {config:?}");

    let time_to_send_txs_from_all_senders =
        (config.tx_batch_size * config.senders) as f64 / config.tps as f64;
    if time_to_send_txs_from_all_senders > config.refresh_delay_secs {
        warn!(
            time_to_send_txs_from_all_senders,
            refresh_delay = config.refresh_delay_secs,
            "Not enough senders for given tps to prevent stall during refresh"
        );
    }

    tokio::spawn(run::run(client, config)).await?
}

fn setup_logging() -> Result<()> {
    use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter, Layer};
    let trace_layer = fmt::layer()
        .with_writer(std::fs::File::create("trace.log")?)
        .with_filter(EnvFilter::new("txgen=trace"));

    let debug_layer = fmt::layer()
        .with_writer(std::fs::File::create("debug.log")?)
        .with_filter(EnvFilter::new("txgen=debug"));

    // log high signal aggregations to stdio
    let stdio_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_filter(EnvFilter::new("txgen=info"));

    // set up subscriber with all layers
    tracing_subscriber::registry()
        .with(trace_layer)
        .with(debug_layer)
        .with(stdio_layer)
        .try_init()
        .map_err(Into::into)
}
