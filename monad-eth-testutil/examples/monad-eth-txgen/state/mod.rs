use std::{collections::HashMap, sync::Arc};

use alloy_rpc_client::ReqwestClient;
use reth_primitives::{revm_primitives::HashSet, Address, TxHash, U256};
use tokio::sync::{RwLock, RwLockReadGuard};
use tracing::warn;

use crate::erc20::ERC20;

use self::manager::{ChainStateManager, ChainStateManagerHandle};

mod blockstream;
mod manager;
pub mod monitors;

#[derive(Default)]
pub struct ChainState {
    erc20_to_check: Option<ERC20>, // generalize to vec
    new_accounts: HashSet<Address>,
    accounts: HashMap<Address, ChainAccountState>,
}

impl ChainState {
    pub async fn new_with_manager(
        client: ReqwestClient,
    ) -> (ChainStateManagerHandle, ChainStateView) {
        let (manager, chain_state) = ChainStateManager::new(client).await;

        (manager.start(), chain_state)
    }

    pub fn get_account(&self, address: &Address) -> Option<&ChainAccountState> {
        self.accounts.get(address)
    }
}

#[derive(Debug)]
pub struct ChainAccountState {
    balance: U256,
    erc20_bal: U256,
    next_nonce: u64,
}

impl ChainAccountState {
    pub fn new(balance: U256, next_nonce: u64) -> Self {
        Self {
            balance,
            next_nonce,
            erc20_bal: U256::ZERO,
        }
    }

    pub fn get_balance(&self) -> U256 {
        self.balance
    }

    pub fn get_next_nonce(&self) -> u64 {
        self.next_nonce
    }

    pub fn get_bal_erc20(&self) -> U256 {
        self.erc20_bal
    }
}

pub type SharedChainState = Arc<RwLock<ChainState>>;

#[derive(Clone)]
pub struct ChainStateView {
    state: Arc<RwLock<ChainState>>,
}

impl ChainStateView {
    pub fn new(state: Arc<RwLock<ChainState>>) -> Self {
        Self { state }
    }

    pub async fn set_erc20(&self, erc20: ERC20) {
        let mut chain_state = self.state.write().await;
        chain_state.erc20_to_check = Some(erc20)
    }

    pub async fn add_new_account(&self, address: Address) {
        let mut chain_state = self.state.write().await;

        if chain_state.accounts.contains_key(&address) {
            warn!(
                "attempted to add new account with address {address:?} when entry already existed"
            );
            return;
        }

        chain_state.new_accounts.insert(address);
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, ChainState> {
        self.state.read().await
    }
}
