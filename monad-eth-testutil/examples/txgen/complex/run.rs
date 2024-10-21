use crate::{manager::TxManager, prelude::*};

pub type BlockNum = U256;
pub type TxMap = Arc<DashMap<TxHash, TxState>>;
pub type AcctMap = Arc<DashMap<Address, GenAccount>>;

pub async fn run(client: ReqwestClient, config: Config) -> Result<()> {
    let (txs, accts) = TxManager::spawn(client).await?;

    Ok(())
}

pub struct TxState {
    pub tx: Tx,
    pub from: Address,
    // applied to local view of state upon committal
    pub update: TxStateUpdate,
    pub sent: Instant,
    pub committed_block: Option<BlockNum>,
}

pub struct Tx {
    pub nonce: u64,
    pub max_gas_cost: U256,
}

pub enum TxStateUpdate {
    NativeTransfer(U256, Address),
    ERC20Mint(U256),
    ERC20Transfer(U256, Address),
}

pub struct GenAccount {
    pub addr: Address,
    pub key: PrivateKey,
    pub committed_balance: Balances,
    pub last_committed_tx: (Option<TxHash>, u64),
    pub in_flight: VecDeque<TxHash>,
}

#[derive(Clone, Default)]
pub struct Balances {
    pub native: U256,
    pub erc20: U256,
}

impl GenAccount {
    pub fn pending_state(&self, txs: &TxMap) -> Option<(Balances, u64)> {
        let mut bal = self.committed_balance.clone();
        for tx_hash in &self.in_flight {
            let tx_state = txs.get(tx_hash)?;
            update_balances(&mut bal, self.addr, &tx_state);
        }
        let nonce = self.last_committed_tx.1 + self.in_flight.len() as u64;
        Some((bal, nonce))
    }

    pub fn commit_tx(&mut self, tx_hash: TxHash, txs: &TxMap) -> Option<()> {
        // if hash not in pending, then either not sent by this account, or already been committed
        let idx = self.in_flight.iter().enumerate().find_map(|(i, hash)| {
            if hash == &tx_hash {
                Some(i)
            } else {
                None
            }
        })?;

        let mut bal = self.committed_balance.clone();
        for _ in 0..=idx {
            let inflight_tx_hash = self.in_flight.pop_front()?;
            let tx_state = txs.get(&inflight_tx_hash)?;
            update_balances(&mut bal, self.addr, &tx_state);
        }

        let committed_tx = txs.get(&tx_hash)?;
        self.committed_balance = bal;
        // we could calculate this from last committed + pending, but let's be explicit
        self.last_committed_tx = (Some(tx_hash), committed_tx.tx.nonce);

        Some(())
    }

    pub fn commit_refresh(&mut self, nonce: u64, native: U256, erc20: Option<U256>) {
        let curr_nonce = self.last_committed_tx.1;

        if curr_nonce > nonce {
            // move 'commited tx' to pending
            self.last_committed_tx
                .0
                .map(|hash| self.in_flight.push_front(hash));
            // set refresh-nonce as true last committed
            self.last_committed_tx = (None, nonce);
        } else if curr_nonce < nonce {
            let diff = nonce - curr_nonce;
            for _ in 0..diff {
                self.in_flight.pop_front();
            }
        }

        self.committed_balance.native = native;
        self.committed_balance.erc20 = erc20.unwrap_or(U256::ZERO);
    }
}

fn update_balances(bal: &mut Balances, addr: Address, tx: &TxState) {
    use TxStateUpdate::*;
    match tx.update {
        NativeTransfer(amt, to) => {
            if tx.from == addr {
                bal.native -= amt + tx.tx.max_gas_cost;
            } else if to == addr {
                bal.native += amt;
            }
        }
        ERC20Mint(amt) => {
            if tx.from == addr {
                bal.native -= tx.tx.max_gas_cost;
                bal.erc20 += amt;
            }
        }
        ERC20Transfer(amt, to) => {
            if tx.from == addr {
                bal.native -= tx.tx.max_gas_cost;
                bal.erc20 -= amt;
            } else if to == addr {
                bal.erc20 += amt;
            }
        }
    }
}
