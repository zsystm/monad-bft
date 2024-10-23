use clap::ValueEnum;
use rand::Rng;
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
    pub metrics: Arc<Metrics>,

    pub recipient_keys: SeededKeyPool,
    pub sender_random_seed: u64,

    pub seed_native_amt: U256,
    pub min_native: U256,
    pub min_erc20: U256,
    pub tx_type: TxType,
    pub sender_group_size: usize,
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

            let mut txs = Vec::with_capacity(accts.len());
            let mut to_addrs = Vec::with_capacity(accts.len());
            for acct in &mut accts {
                let (t, a) = self.generate(acct).await;
                txs.push(t);
                to_addrs.push(a);
            }

            let num_txs = txs.len();

            self.rpc_sender
                .send(AccountsWithTxs {
                    accts,
                    txs,
                    to_addrs,
                })
                .await
                .expect("rpc sender channel closed");

            debug!(num_txs, "Gen pushed txs to rpc sender");
        }
    }

    pub async fn generate(
        &mut self,
        sender: &mut SimpleAccount,
    ) -> (Vec<TransactionSigned>, Vec<Address>) {
        // ensure sender stays within batch boundary
        let mut txs = Vec::with_capacity(BATCH_SIZE);
        let mut recipients = Vec::with_capacity(BATCH_SIZE);

        // fixme: don't just always mint...
        if let TxType::ERC20 = self.tx_type {
            txs.push(erc20_mint(
                sender,
                self.min_erc20 * U256::from(10),
                &self.erc20,
            ));
            recipients.push(sender.addr);
        }

        for _ in txs.len()..BATCH_SIZE {
            if sender.native_bal < self.min_native {
                debug!(
                    bal = sender.native_bal.to::<u128>(),
                    min = self.min_native.to::<u128>(),
                    addr = "Sender native bal below minimum, breaking gen loop"
                );
                // todo: refill from root somewhere
                break;
            }

            // if sender.erc20_bal < self.min_erc20 {
            //     txs.push(erc20_mint(
            //         sender,
            //         self.min_erc20 * U256::from(10),
            //         &self.erc20,
            //     ));
            //     recipients.push(sender.addr);
            //     break;
            // }

            let to = self.recipient_keys.next_addr();

            txs.push(match self.tx_type {
                TxType::ERC20 => erc20_transfer(sender, to, U256::from(10), &self.erc20),
                TxType::Native => native_transfer(sender, to, U256::from(10)),
            });

            recipients.push(to);
        }
        (txs, recipients)
    }

    pub async fn seed(&mut self, num_senders: usize) {
        info!(num_senders = num_senders, "Generating sender keys...");
        let mut rng = SmallRng::seed_from_u64(self.sender_random_seed);
        let to_seed: Vec<_> = generate_keys(&mut rng, num_senders).collect();
        let mut root_slice = vec![std::mem::take(&mut self.root)]; // can't move out of array, so make it a vec...

        info!("Seeding...");
        for batch in to_seed.chunks(BATCH_SIZE) {
            info!(batch_size = batch.len(), "Seeding batch...");

            let mut txs = Vec::with_capacity(BATCH_SIZE);

            let pre_nonce = root_slice[0].nonce;
            for acct in batch.iter() {
                txs.push(native_transfer(
                    &mut root_slice[0],
                    acct.0,
                    self.seed_native_amt,
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
                let _ = refresh_batch(
                    &self.client,
                    &self.erc20,
                    &mut root_slice,
                    &self.metrics,
                    false,
                )
                .await;

                if root_slice[0].nonce > pre_nonce {
                    info!(
                        pre_nonce,
                        nonce = root_slice[0].nonce,
                        "Observed root account with updated nonce"
                    );
                    if root_slice[0].nonce - pre_nonce < batch.len() as u64 {
                        warn!("Expected root nonce to be incremented by number of accts to be seeded in batch");
                    }
                    info!("Root {}", &root_slice[0]);
                    break;
                }
            }

            info!("Sending seed accts to rpc sender with no txs...");
            for group in batch.chunks(self.sender_group_size.min(BATCH_SIZE)) {
                self.rpc_sender
                    .send(AccountsWithTxs {
                        accts: group.iter().cloned().map(SimpleAccount::from).collect(),
                        txs: Vec::new(),
                        to_addrs: vec![group.iter().map(|(a, _)| *a).collect()],
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

    let sig = from.key.sign_transaction(&tx);
    TransactionSigned::from_transaction_and_signature(tx, sig)
}

pub fn erc20_transfer(
    from: &mut SimpleAccount,
    to: Address,
    amt: U256,
    erc20: &ERC20,
) -> TransactionSigned {
    let tx = erc20.construct_transfer(&from.key, to, from.nonce, amt);

    // update from
    from.nonce += 1;
    from.native_bal -= U256::from(400_000 * 1_000); // todo: fixme
    from.erc20_bal -= amt;

    tx
}

pub fn erc20_mint(from: &mut SimpleAccount, amt: U256, erc20: &ERC20) -> TransactionSigned {
    let tx = erc20.construct_mint(&from.key, from.nonce);

    // update from
    from.nonce += 1;
    from.native_bal -= U256::from(400_000 * 1_000); // todo: fixme
    from.erc20_bal += amt; // todo: fixme

    tx
}

pub fn generate_keys<'a>(
    rng: &'a mut impl Rng,
    num_keys: usize,
) -> impl Iterator<Item = (Address, PrivateKey)> + 'a {
    (0..num_keys).map(|_| PrivateKey::new_with_random(rng))
}
