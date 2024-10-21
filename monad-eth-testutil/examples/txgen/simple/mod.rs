use std::iter;

use futures::{stream::FuturesUnordered, StreamExt};
use rand::rngs::SmallRng;
use reth_primitives::TransactionSigned;
use serde::Deserialize;

use crate::{
    prelude::*,
    shared::{erc20::ERC20, json_rpc::JsonRpc},
};

const BATCH_SIZE: usize = 500;

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
    txs: Vec<TransactionSigned>,
}

pub struct Refresher {
    pub rpc_rx: mpsc::Receiver<AccountsWithTime>,
    pub gen_sender: mpsc::Sender<Accounts>,

    pub client: ReqwestClient,
    pub erc20: ERC20,

    pub delay: Duration,
}

pub struct RpcSender {
    pub gen_rx: mpsc::Receiver<AccountsWithTxs>,
    pub refresh_sender: mpsc::Sender<AccountsWithTime>,

    pub client: ReqwestClient,
    pub target_tps: u64,
}

pub struct Generator {
    pub refresh_rx: mpsc::Receiver<Accounts>,
    pub rpc_sender: mpsc::Sender<AccountsWithTxs>,
    pub recipient_sender: mpsc::Sender<AccountsWithTime>,

    pub client: ReqwestClient,
    pub erc20: ERC20,
    pub root: SimpleAccount,
    pub last_used_root: Instant, // todo: figure out a better way to do root refreshes...
    pub to_generator: ToAcctGenerator,

    pub seed_native: U256,
    pub min_native: U256,
    pub min_erc20: U256,
    pub mode: GenMode,
}

pub struct ToAcctGenerator {
    pub seed: u64,
    pub rng: SmallRng,
    pub buf: VecDeque<SimpleAccount>,
}

impl ToAcctGenerator {
    fn gen(&mut self) -> Address {
        let (addr, key) = PrivateKey::new_with_random(&mut self.rng);
        self.buf.push_back(SimpleAccount {
            nonce: 0,
            native_bal: U256::ZERO,
            erc20_bal: U256::ZERO,
            key,
            addr,
        });
        addr
    }
}

#[derive(Deserialize, Debug)]
pub enum GenMode {
    ERC20,
    Native,
}

pub struct RecipientTracker {
    pub gen_rx: mpsc::Receiver<AccountsWithTime>,
}
impl RecipientTracker {
    pub async fn run(mut self) {
        while let Some(_accts) = self.gen_rx.recv().await {
            // todo: track em
        }
    }
}

impl Generator {
    pub async fn run(mut self, num_senders: usize) {
        iter::repeat_with(|| self.to_generator.gen())
            .take(num_senders)
            .count();

        self.seed(self.to_generator.buf.clone().into()).await;

        while let Some(mut accts) = self.refresh_rx.recv().await {
            let mut txs = Vec::with_capacity(accts.len() * BATCH_SIZE);
            for acct in &mut accts {
                self.generate(acct, &mut txs);
            }

            self.rpc_sender
                .send(AccountsWithTxs { accts, txs })
                .await
                .expect("rpc sender channel closed");
        }
    }

    pub async fn seed(&mut self, mut to_seed: Vec<SimpleAccount>) {
        let mut root_slice = vec![std::mem::take(&mut self.root)]; // can't move out of array, so make it a vec...

        for batch in to_seed.chunks_mut(BATCH_SIZE) {
            let mut txs = Vec::with_capacity(BATCH_SIZE);

            let pre_nonce = root_slice[0].nonce;
            for acct in batch.iter_mut() {
                txs.push(native_transfer(
                    &mut root_slice[0],
                    acct.addr,
                    self.seed_native,
                ));
            }

            send_batch(&self.client, &txs).await;

            // speed focussed version of refresh loop for root
            for _polls in 0..50 {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let _ = refresh_batch(&self.client, &self.erc20, &mut root_slice).await;

                if root_slice[0].nonce > pre_nonce {
                    break;
                }
            }

            for group in batch.chunks(50) {
                self.rpc_sender
                    .send(AccountsWithTxs {
                        accts: group.iter().cloned().collect(),
                        txs: Vec::new(),
                    })
                    .await
                    .unwrap();
            }
        }

        self.root = root_slice.pop().unwrap();
    }

    pub fn generate(&mut self, sender: &mut SimpleAccount, txs: &mut Vec<TransactionSigned>) {
        // ensure sender stays within batch boundary
        let start_len = txs.len();
        let to_add = BATCH_SIZE - start_len % BATCH_SIZE;
        for _ in 0..to_add {
            // respect BATCH_SIZE txs boundary
            if sender.native_bal < self.min_native {
                break;
            }
            txs.push(native_transfer(
                sender,
                self.to_generator.gen(),
                U256::from(10),
            ));
        }
    }
}

impl Refresher {
    pub async fn run(mut self) {
        while let Some(AccountsWithTime { mut accts, sent }) = self.rpc_rx.recv().await {
            if sent + self.delay >= Instant::now() {
                tokio::time::sleep_until(sent + self.delay).await;
            }

            let mut times_sent = 0;
            while let Err(e) = refresh_batch(&self.client, &self.erc20, &mut accts).await {
                if times_sent < 5 {
                    error!("Exhausted retries refreshing account, oh well! {e}");
                } else {
                    times_sent += 1;
                    warn!(
                        times_sent,
                        "Encountered error refreshing accts, retrying..., {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }
}

async fn refresh_batch(
    client: &ReqwestClient,
    erc20: &ERC20,
    accts: &mut [SimpleAccount],
) -> Result<()> {
    let str_addrs = accts.iter().map(|a| a.addr.to_string()).collect::<Vec<_>>();
    let addrs = accts.iter().map(|a| a.addr).collect::<Vec<_>>();

    let (erc20_res, native_bals, nonces) = tokio::join!(
        client.batch_get_erc20_balance(&addrs, *erc20),
        client.batch_get_balance(&str_addrs),
        client.batch_get_transaction_count(&str_addrs),
    );

    let native_bals = native_bals?;
    let nonces = nonces?;
    let erc20_bals = erc20_res?;

    for (i, acct) in accts.iter_mut().enumerate() {
        if let Ok(b) = &erc20_bals[i] {
            acct.erc20_bal = *b;
        }
        if let Ok(b) = &native_bals[i] {
            acct.native_bal = *b;
        }
        if let Ok(n) = &nonces[i] {
            acct.nonce = *n;
        }
    }

    Ok(())
}

impl RpcSender {
    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(Duration::from_millis(
            BATCH_SIZE as u64 * 1000 / self.target_tps,
        ));

        while let Some(AccountsWithTxs { accts, txs }) = self.gen_rx.recv().await {
            for batch in txs.chunks(BATCH_SIZE) {
                if batch.is_empty() {
                    break; // unnecessary?
                }
                send_batch(&self.client, batch).await;

                // limit sending batch by interval
                interval.tick().await;
            }

            self.refresh_sender
                .send(AccountsWithTime {
                    accts,
                    sent: Instant::now(),
                })
                .await
                .expect("Sender not closed");
        }
    }
}

async fn send_batch(client: &ReqwestClient, txs: &[TransactionSigned]) {
    let mut batch_req = client.new_batch();
    let num_txs = txs.len();

    let futs = txs
        .into_iter()
        .filter_map(|tx| {
            batch_req
                .add_call::<_, TxHash>("eth_sendRawTransaction", &[tx.envelope_encoded()])
                .ok() // todo: handle better
        })
        .collect::<FuturesUnordered<_>>();

    if let Err(e) = batch_req.send().await {
        error!("Failed to send batch: {e}");
        return;
    }

    let num_resp = futs.count().await;
    if num_txs != num_resp {
        debug!(
            num_txs,
            num_resp, "Expected all txs from batch to make it or all to not"
        );
    }
}

fn native_transfer(from: &mut SimpleAccount, to: Address, amt: U256) -> TransactionSigned {
    let tx = reth_primitives::Transaction::Eip1559(reth_primitives::TxEip1559 {
        chain_id: 41454,
        nonce: from.nonce,
        gas_limit: 21_000,
        max_fee_per_gas: 1_000,
        max_priority_fee_per_gas: 0,
        to: reth_primitives::TransactionKind::Call(to),
        value: amt.clone().into(),
        access_list: reth_primitives::AccessList::default(),
        input: Default::default(),
    });

    // update from
    from.nonce += 1;
    from.native_bal -= amt + U256::from(21_000 * 1_000);

    let sig = from.key.sign_transaction(&tx).unwrap();
    TransactionSigned::from_transaction_and_signature(tx, sig)
}
