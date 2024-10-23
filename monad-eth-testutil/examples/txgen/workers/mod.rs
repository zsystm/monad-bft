use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use futures::{stream::FuturesUnordered, StreamExt};
use rand::{rngs::SmallRng, SeedableRng};
use reth_primitives::TransactionSigned;

use crate::{
    prelude::*,
    shared::{erc20::ERC20, json_rpc::JsonRpc},
};

pub mod committed_tx_watcher;
pub mod gen;
pub mod metrics;
pub mod recipient_tracker;
pub mod refresher;
pub mod rpc_sender;

pub use committed_tx_watcher::*;
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

pub struct SeededKeyPool {
    pub rng: SmallRng,
    pub buf: Vec<(Address, PrivateKey)>,
    pub cursor: usize,
}

impl SeededKeyPool {
    pub fn new(num_keys: usize, seed: u64) -> SeededKeyPool {
        let mut buf = Vec::with_capacity(num_keys);
        let mut rng = SmallRng::seed_from_u64(seed);
        buf.push(PrivateKey::new_with_random(&mut rng));
        SeededKeyPool {
            rng,
            buf,
            cursor: 0,
        }
    }

    fn next_idx(&mut self) -> usize {
        if self.buf.len() < self.buf.capacity() {
            self.buf.push(PrivateKey::new_with_random(&mut self.rng));
        }
        let idx = self.cursor;
        self.cursor = (self.cursor + 1) % self.buf.capacity();
        idx
    }

    pub fn next_addr(&mut self) -> Address {
        let idx = self.next_idx();
        self.buf[idx].0
    }

    pub fn next_key(&mut self) -> (Address, PrivateKey) {
        let idx = self.next_idx();
        self.buf[idx].clone()
    }
}

pub struct AsyncSeededKeyPool {
    pub rx: mpsc::Receiver<(Address, PrivateKey)>,
    pub buf: Vec<(Address, PrivateKey)>,
    pub cursor: usize,
}

impl AsyncSeededKeyPool {
    pub fn new(num_keys: usize, seed: u64) -> AsyncSeededKeyPool {
        let mut buf = Vec::with_capacity(num_keys);
        let mut rng = SmallRng::seed_from_u64(seed);
        buf.push(PrivateKey::new_with_random(&mut rng));

        let (sender, rx) = mpsc::channel(num_keys.min(10000));
        tokio::task::spawn_blocking(move || {
            for _ in 1..num_keys {
                let _ = sender.blocking_send(PrivateKey::new_with_random(&mut rng));
            }
        });

        Self { rx, buf, cursor: 0 }
    }

    async fn next_idx(&mut self) -> usize {
        if let Some(x) = self.rx.recv().await {
            self.buf.push(x);
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
    fn test_sync_pool() {
        let mut idxs = Vec::with_capacity(100);
        let mut gen = SeededKeyPool::new(11, 1);

        for i in 0..12 {
            idxs.push(gen.next_idx());
        }

        assert_eq!(idxs, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0]);

        idxs.clear();
        for i in 0..12 {
            idxs.push(gen.next_idx());
        }

        assert_eq!(idxs, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1]);
    }

    #[tokio::test]
    async fn test_async_pool() {
        let mut idxs = Vec::with_capacity(100);
        let mut gen = AsyncSeededKeyPool::new(11, 1);

        for i in 0..12 {
            idxs.push(gen.next_idx().await);
        }

        assert_eq!(idxs, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0]);

        idxs.clear();
        for i in 0..12 {
            idxs.push(gen.next_idx().await);
        }

        assert_eq!(idxs, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1]);
    }
}
