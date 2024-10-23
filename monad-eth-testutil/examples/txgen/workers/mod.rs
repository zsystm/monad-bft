use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use futures::{stream::FuturesUnordered, StreamExt};
use rand::{rngs::SmallRng, SeedableRng};
use reth_primitives::TransactionSigned;

use crate::{
    prelude::*,
    shared::{erc20::ERC20, json_rpc::JsonRpc},
};

pub mod gen;
pub mod metrics;
pub mod recipient_tracker;
pub mod refresher;
pub mod rpc_sender;

pub use gen::*;
pub use metrics::*;
pub use recipient_tracker::*;
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

pub struct AddrsWithTime {
    addrs: Vec<Address>,
    sent: Instant,
}

pub struct AccountsWithTxs {
    accts: Accounts,
    txs: Vec<Vec<TransactionSigned>>,
    to_addrs: Vec<Vec<Address>>,
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

impl From<(Address, PrivateKey)> for SimpleAccount {
    fn from((addr, key): (Address, PrivateKey)) -> Self {
        SimpleAccount {
            key,
            addr,
            ..Default::default()
        }
    }
}

pub struct AsyncSeededKeyPool {
    pub num_keys: usize,
    pub rx: mpsc::Receiver<(Address, PrivateKey)>,
    pub buf: Vec<(Address, PrivateKey)>,
    pub cursor: usize,
}

impl AsyncSeededKeyPool {
    pub fn new(num_keys: usize, seed: u64) -> AsyncSeededKeyPool {
        let buf = Vec::with_capacity(num_keys);

        let (sender, rx) = mpsc::channel(num_keys.min(10000));
        tokio::task::spawn_blocking(move || {
            let mut rng = SmallRng::seed_from_u64(seed);
            for _ in 0..num_keys {
                let _ = sender.blocking_send(PrivateKey::new_with_random(&mut rng));
            }
        });

        Self {
            rx,
            buf,
            cursor: 0,
            num_keys,
        }
    }

    async fn next_idx(&mut self) -> usize {
        if self.cursor == self.buf.len() {
            let num_recvd = self.rx.recv_many(&mut self.buf, 10).await;
            if num_recvd == 0 {
                self.cursor = 0;
            }
        }

        let idx = self.cursor;
        self.cursor = (self.cursor + 1) % self.buf.len();
        idx
    }

    pub async fn next_addr(&mut self) -> Address {
        let idx = self.next_idx().await;
        self.buf[idx].0
    }

    pub async fn next_key(&mut self) -> (Address, PrivateKey) {
        let idx = self.next_idx().await;
        self.buf[idx].clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    async fn test_acct_gen() {
        let mut idxs = Vec::with_capacity(100);
        let gen = AsyncSeededKeyPool::new(11, 1);

        for i in 0..12 {
            idxs.push(gen.next_idx().await);
        }

        assert_eq!(idxs, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0]);
    }
}
