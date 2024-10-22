use std::iter;

use clap::ValueEnum;
use rand::rngs::SmallRng;
use refresher::refresh_batch;
use reth_primitives::TransactionSigned;
use rpc_sender::send_batch;
use serde::Deserialize;

use crate::{prelude::*, shared::erc20::ERC20};

use super::*;

pub struct Generator {
    pub refresh_rx: mpsc::Receiver<Accounts>,
    pub rpc_sender: mpsc::Sender<AccountsWithTxs>,

    pub client: ReqwestClient,
    pub erc20: ERC20,
    pub root: SimpleAccount,
    pub last_used_root: Instant, // todo: figure out a better way to do root refreshes...
    pub to_generator: ToAcctGenerator,
    pub metrics: Arc<Metrics>,

    pub seed_native: U256,
    pub min_native: U256,
    pub min_erc20: U256,
    pub mode: TxType,
}

pub struct ToAcctGenerator {
    pub seed: u64,
    pub rng: SmallRng,
    pub buf: Vec<SimpleAccount>,
}

impl ToAcctGenerator {
    fn gen(&mut self) -> Address {
        let (addr, key) = PrivateKey::new_with_random(&mut self.rng);
        self.buf.push(SimpleAccount {
            nonce: 0,
            native_bal: U256::ZERO,
            erc20_bal: U256::ZERO,
            key,
            addr,
        });
        addr
    }

    pub fn drain(&mut self) -> Vec<SimpleAccount> {
        std::mem::take(&mut self.buf)
    }
}

#[derive(Deserialize, Clone, Copy, Debug, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum TxType {
    ERC20,
    Native,
}

impl Generator {
    pub async fn run(mut self, num_senders: usize) {
        self.seed(num_senders).await;

        info!("Starting main gen loop");
        while let Some(mut accts) = self.refresh_rx.recv().await {
            info!(
                num_accts = accts.len(),
                channel_len = self.refresh_rx.len(),
                "Gen recv'd accts from refresher"
            );

            let (txs, to_accts): (Vec<_>, Vec<_>) =
                accts.iter_mut().map(|a| self.generate(a)).unzip();

            let num_txs = txs.len();

            self.rpc_sender
                .send(AccountsWithTxs {
                    accts,
                    txs,
                    to_accts,
                })
                .await
                .expect("rpc sender channel closed");

            debug!(num_txs, "Gen pushed txs to rpc sender");
        }
    }

    pub fn generate(&mut self, sender: &mut SimpleAccount) -> (Vec<TransactionSigned>, Accounts) {
        // ensure sender stays within batch boundary
        let mut txs = Vec::with_capacity(BATCH_SIZE);
        for _ in 0..BATCH_SIZE {
            if sender.native_bal < self.min_native {
                debug!(
                    bal = sender.native_bal.to::<u128>(),
                    min = self.min_native.to::<u128>(),
                    addr = "Sender native bal below minimum, breaking gen loop"
                );
                // todo: refill from root somewhere
                break;
            }

            txs.push(native_transfer(
                sender,
                self.to_generator.gen(),
                U256::from(10),
            ));
        }
        (txs, self.to_generator.drain())
    }

    pub async fn seed(&mut self, num_senders: usize) {
        iter::repeat_with(|| self.to_generator.gen())
            .take(num_senders)
            .count();
        let mut to_seed = self.to_generator.buf.clone();
        info!(num_to_seed = to_seed.len(), "Seeding...");

        let mut root_slice = vec![std::mem::take(&mut self.root)]; // can't move out of array, so make it a vec...

        for batch in to_seed.chunks_mut(BATCH_SIZE) {
            info!(batch_size = batch.len(), "Seeding batch...");

            let mut txs = Vec::with_capacity(BATCH_SIZE);

            let pre_nonce = root_slice[0].nonce;
            for acct in batch.iter_mut() {
                txs.push(native_transfer(
                    &mut root_slice[0],
                    acct.addr,
                    self.seed_native,
                ));
            }

            info!("Sending seed txs...");
            send_batch(&self.client, txs, &self.metrics).await;
            info!("Seed txs sent");

            // speed focussed version of refresh loop for root
            for poll_attempt in 1..=50 {
                tokio::time::sleep(Duration::from_millis(200)).await;

                debug!(
                    poll_attempt,
                    "Polling for root account with updated nonce..."
                );
                let _ =
                    refresh_batch(&self.client, &self.erc20, &mut root_slice, &self.metrics).await;

                if root_slice[0].nonce > pre_nonce {
                    info!(
                        pre_nonce,
                        nonce = root_slice[0].nonce,
                        "Observed root account with updated nonce"
                    );
                    info!("Root {}", &root_slice[0]);
                    break;
                }
            }

            info!("Sending seed accts to rpc sender with no txs...");
            for group in batch.chunks(50) {
                self.rpc_sender
                    .send(AccountsWithTxs {
                        accts: group.iter().cloned().collect(),
                        txs: Vec::new(),
                        to_accts: vec![self.to_generator.drain()],
                    })
                    .await
                    .unwrap();
            }
            info!("Seed accts sent to rpc sender");
        }
        info!("Seeding complete");

        self.root = root_slice.pop().unwrap();
    }
}

pub fn native_transfer(from: &mut SimpleAccount, to: Address, amt: U256) -> TransactionSigned {
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
