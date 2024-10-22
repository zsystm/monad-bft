use std::{
    iter,
    sync::atomic::{AtomicUsize, Ordering::SeqCst},
};

use futures::{stream::FuturesUnordered, StreamExt};
use monad_consensus_types::metrics;
use rand::rngs::SmallRng;
use reth_primitives::TransactionSigned;
use serde::Deserialize;

use crate::{
    prelude::*,
    shared::{erc20::ERC20, json_rpc::JsonRpc},
};

pub mod gen;
pub mod refresher;
pub mod rpc_sender;
pub mod recipient_tracker;
pub mod metrics;

pub use gen::*;
pub use refresher::*;
pub use rpc_sender::*;

pub const BATCH_SIZE: usize = 500;

#[derive(Clone, Default)]
pub struct SimpleAccount {
    pub nonce: u64,
    pub native_bal: U256,
    pub erc20_bal: U256,
    pub key: PrivateKey,
    pub addr: Address,
}

pub type Accounts = Vec<SimpleAccount>;

pub struct AccountsWithTime {
    accts: Accounts,
    sent: Instant,
}

pub struct AccountsWithTxs {
    accts: Accounts,
    txs: Vec<Vec<TransactionSigned>>,
    to_accts: Vec<Accounts>,
}


impl std::fmt::Display for SimpleAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Account{{ nonce: {}, native: {:+.2e}, erc20: {:+.2e}, addr: {}}}",
            self.nonce,
            self.native_bal.to::<u128>(),
            self.erc20_bal.to::<u128>(),
            self.addr,
        )
    }
}
