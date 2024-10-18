use std::error::Error;

use ruint::aliases::U256;
use serde::Deserialize;

use crate::erc20::ERC20;

#[derive(Deserialize, Debug)]
pub struct EthTxGeneratorConfig {
    pub root_private_key: String,
    pub target_tps: f64,
    pub addresses: EthTxAddressConfig,
    pub activity: EthTxActivityType,
}

impl EthTxGeneratorConfig {
    pub fn new_from_file(config_path: std::path::PathBuf) -> Result<Self, Box<dyn Error>> {
        let config = std::fs::read_to_string(config_path)?;

        Ok(serde_json::from_str(&config)?)
    }
}

#[derive(Deserialize, Debug)]
pub struct EthTxAddressConfig {
    pub(super) from: EthTxAddressPoolConfig,
    pub(super) to: EthTxAddressPoolConfig,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EthTxAddressPoolConfig {
    Single { private_key: String },
    RandomSeeded { seed: u64, count: usize },
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
