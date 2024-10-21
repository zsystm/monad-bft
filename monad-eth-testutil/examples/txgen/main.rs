#![allow(async_fn_in_trait)]

use std::path::PathBuf;

use alloy_rpc_client::ClientBuilder;
use clap::Parser;
use prelude::*;
use serde::Deserialize;
use shared::erc20::ERC20;
use simple::GenMode;
use tracing_subscriber::util::SubscriberInitExt;
use url::Url;

// pub mod complex;
pub mod prelude;
pub mod run;
pub mod shared;
pub mod simple;

#[derive(Debug, Parser)]
#[command(name = "monad-node", about, long_about = None)]
pub struct Cli {
    #[arg(long, default_value = "http://localhost:8080")]
    pub rpc_url: Url,

    #[arg(long)]
    pub config: PathBuf,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub num_senders: usize,
    pub seed: u64,
    pub mode: GenMode,
    pub refresh_delay_secs: f64,
    pub target_tps: u64,
    pub root_private_key: String,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EthTxActivityType {
    NativeTokenTransfer {
        quantity: U256,
    },
    Erc20TokenTransfer {
        contract: Option<ERC20>,
        quantity: U256,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    setup_logging()?;

    let args = Cli::parse();
    // let config = EthTxGeneratorConfig::new_from_file(args.config).expect("Failed to load config");
    let config = Config {
        num_senders: 500,
        seed: 1,
        mode: GenMode::Native,
        refresh_delay_secs: 5.,
        target_tps: 1000,
        root_private_key: "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
            .to_string(),
    };
    let client: ReqwestClient = ClientBuilder::default().http(args.rpc_url);

    // info!("Config: {config:?}");

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
        .with_filter(EnvFilter::new("monad_eth_txgen=info"));

    // set up subscriber with all layers
    tracing_subscriber::registry()
        .with(trace_layer)
        .with(debug_layer)
        .with(stdio_layer)
        .try_init()
        .map_err(Into::into)
}
