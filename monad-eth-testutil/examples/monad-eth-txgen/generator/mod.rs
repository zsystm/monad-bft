use std::{collections::HashMap, time::Duration};

use config::EthTxActivityType;
use reth_primitives::{
    AccessList, Address, Transaction, TransactionKind, TransactionSigned, TxEip1559, U256,
};
use ruint::Uint;
use tokio::time::{Instant, Interval};
use tracing::{error, info, instrument, span, trace, Level};

pub use self::config::EthTxGeneratorConfig;
use self::{account::EthAccount, account_pool::AccountPool, config::EthTxAddressConfig};
use crate::{
    account::PrivateKey,
    state::{ChainState, ChainStateView},
};

mod account;
mod account_pool;
mod config;

const TX_LIMIT: usize = 500;

#[derive(Debug)]
pub struct EthTxGenerator {
    root_account: (Address, EthAccount),
    account_pool: AccountPool,
    chain_state: ChainStateView,
    activity: EthTxActivityType,
    interval: Interval,

    // metrics
    txs_sent: usize,
    seed_txs_sent: usize,
    txs_sent_at_reported: usize,
    last_called_time: Instant,
}

impl EthTxGenerator {
    pub async fn new(config: EthTxGeneratorConfig, chain_state: ChainStateView) -> Self {
        let EthTxGeneratorConfig {
            root_private_key,
            addresses: EthTxAddressConfig { from, to },
            activity,
            target_tps,
        } = config;

        let (root_account_address, root_account) = PrivateKey::new(root_private_key);
        chain_state.add_new_account(root_account_address).await;

        let account_pool = AccountPool::new_with_config(from, to, &chain_state).await;
        let interval =
            tokio::time::interval(Duration::from_secs_f64(TX_LIMIT as f64 / target_tps as f64));

        info!(
            "Interval set to {} ms with {} batch size to target {} tps",
            interval.period().as_millis(),
            TX_LIMIT,
            target_tps
        );

        Self {
            root_account: (root_account_address, EthAccount::new(root_account)),
            account_pool,
            chain_state,
            activity,
            interval,
            // metrics
            txs_sent: 0,
            seed_txs_sent: 0,
            last_called_time: Instant::now(),
            txs_sent_at_reported: 0,
        }
    }

    pub async fn tick(&mut self) {
        self.interval.tick().await;
        // tokio::time::sleep(Duration::from_millis(100)).await;
    }

    pub async fn generate(&mut self) -> Vec<TransactionSigned> {
        let mut txs = Vec::with_capacity(TX_LIMIT);
        trace!("Starting generate");

        match &self.activity {
            EthTxActivityType::NativeTokenTransfer { quantity } => {
                let chain_state = self.chain_state.read().await;
                let (iter, accts) = self.account_pool.iter_random(0xFFFF);

                for (from_address, to_address) in iter {
                    let Some(tx) = EthTxGenerator::generate_native_tx(
                        &mut self.root_account,
                        quantity.clone(),
                        &chain_state,
                        accts,
                        from_address,
                        to_address,
                        &mut self.seed_txs_sent,
                    ) else {
                        continue;
                    };

                    txs.push(tx);
                }
            }
            EthTxActivityType::Erc20TokenTransfer { .. } => unimplemented!(),
        }

        // metrics
        self.txs_sent += txs.len();
        let now = Instant::now();
        let elapsed = (now - self.last_called_time).as_millis();
        if elapsed > 1000 {
            let recent_txs = self.txs_sent - self.txs_sent_at_reported;
            let tps = recent_txs as u128 * 1000 / elapsed;

            info!(
                "Sent {:.5} txs in {elapsed:.5} ms for tps of {:.5}. Total seed txs {:.5}",
                recent_txs, tps, self.seed_txs_sent
            );

            self.txs_sent_at_reported = self.txs_sent;
            self.last_called_time = now;
        }

        trace!("End generate");
        txs
    }

    fn generate_native_tx(
        root_account: &mut (Address, EthAccount),
        quantity: U256,
        chain_state: &ChainState,
        accts: &mut HashMap<Address, EthAccount>,
        from_address: Address,
        to_address: Address,
        seed_txs_sent: &mut usize,
    ) -> Option<TransactionSigned> {
        let from_account = accts.get_mut(&from_address).unwrap();
        let from_account_state = chain_state.get_account(&from_address)?;

        let insufficient_balance = from_account_state
            .get_balance()
            .le(&Uint::from(1_000_000_000u64));

        if insufficient_balance {
            // seed account from root
            let tx = native_transfer_tx(
                root_account.0,
                &mut root_account.1,
                from_address,
                &chain_state,
                quantity.clone() * Uint::from(10000),
            );
            if tx.is_some() {
                *seed_txs_sent += 1; // can we avoid this ugliness?
            }
            tx
        } else {
            native_transfer_tx(
                from_address,
                from_account,
                to_address,
                &chain_state,
                quantity.clone(),
            )
        }
    }
}

fn native_transfer_tx(
    from: Address,
    from_acct: &mut EthAccount,
    to: Address,
    chain_state: &ChainState,
    quantity: U256,
) -> Option<TransactionSigned> {
    chain_state.get_account(&from)?;

    let nonce = from_acct.get_available_nonce(chain_state.get_account(&from)?, 512)?;

    let transaction = Transaction::Eip1559(TxEip1559 {
        chain_id: 41454,
        nonce,
        gas_limit: 21_000,
        max_fee_per_gas: 1_000,
        max_priority_fee_per_gas: 0,
        to: TransactionKind::Call(to),
        value: quantity.into(),
        access_list: AccessList::default(),
        input: Vec::default().into(),
    });

    let signature = match from_acct.sign_transaction(&transaction) {
        Ok(s) => s,
        Err(e) => {
            error!(
                signer_pub_key = from.to_string(),
                "Error signing transaction during generation: {e}"
            );
            return None;
        }
    };

    let signed_transaction =
        TransactionSigned::from_transaction_and_signature(transaction, signature);

    return Some(signed_transaction);
}
