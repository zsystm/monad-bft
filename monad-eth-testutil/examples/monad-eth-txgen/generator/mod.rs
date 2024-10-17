use std::time::Duration;

use config::EthTxActivityType;
use reth_primitives::{
    AccessList, Address, Transaction, TransactionKind, TransactionSigned, TxEip1559, U256,
};
use ruint::Uint;
use tokio::time::Instant;
use tracing::{error, trace};

pub use self::config::EthTxGeneratorConfig;
use self::{account::EthAccount, account_pool::AccountPool, config::EthTxAddressConfig};
use crate::{account::PrivateKey, state::ChainStateView};

mod account;
mod account_pool;
mod config;

pub struct EthTxGenerator {
    root_account: (Address, EthAccount),
    pub account_pool: AccountPool,
    chain_state: ChainStateView,
    activity: EthTxActivityType,

    // throttle logs
    counter: usize,
}

impl EthTxGenerator {
    pub async fn new(
        config: EthTxGeneratorConfig,
        chain_state: ChainStateView,
    ) -> (Self, tokio::time::Interval) {
        let EthTxGeneratorConfig {
            root_private_key,
            addresses: EthTxAddressConfig { from, to },
            activity,
            target_tps,
        } = config;

        let (root_account_address, root_account) = PrivateKey::new(root_private_key);
        chain_state.add_new_account(root_account_address).await;

        let account_pool = AccountPool::new_with_config(from, to, &chain_state).await;

        (
            Self {
                root_account: (root_account_address, EthAccount::new(root_account)),
                account_pool,
                chain_state,
                activity,
                // throttle logs
                counter: 0,
            },
            tokio::time::interval(Duration::from_secs_f64(500. / target_tps)),
        )
    }

    pub async fn generate(&mut self) -> Vec<TransactionSigned> {
        self.account_pool
            .process_async_registrations(&self.chain_state)
            .await;

        let mut txs = Vec::default();

        match &self.activity {
            EthTxActivityType::NativeTokenTransfer { quantity } => {
                let chain_state = self.chain_state.read().await;
                // trace!("Calling iter random");
                self.account_pool
                    .iter_random(20_000, |from_address, from_account, to_address| {
                        if txs.len() >= 500 {
                            return false;
                        }
                        self.counter += 1;

                        let should_trace = self.counter % 24_391 == 0;
                        if should_trace {
                            trace!("Top of generate closure in iter random");
                            trace!("from: {}: {}", format_addr(&from_address), from_account);
                            trace!("To: {}", format_addr(&to_address));
                        }

                        let Some(from_account_state) = chain_state.get_account(&from_address)
                        else {
                            if should_trace {
                                trace!("Skipping from acct, not in chain state");
                            }
                            return true;
                        };

                        if from_account_state
                            .get_balance()
                            .le(&Uint::from(1_000_000_000u64))
                        {
                            if should_trace {
                                trace!(
                                    "root: {}: {}",
                                    format_addr(&self.root_account.0),
                                    self.root_account.1
                                );
                                trace!(
                                    bal = from_account_state.get_balance().to_string(),
                                    from = format_addr(&from_address),
                                    "Seeding acct from root bc balance is too low"
                                );
                            }

                            let Some(root_account_state) =
                                chain_state.get_account(&self.root_account.0)
                            else {
                                error!("Root account state not found");
                                return true;
                            };

                            let Some(nonce) = self.root_account.1.get_available_nonce(
                                root_account_state,
                                512,
                                should_trace,
                            ) else {
                                if should_trace {
                                    trace!(
                                        "Skipping seeding from root bc root nonce not available"
                                    );
                                }
                                return true;
                            };

                            let transaction = Transaction::Eip1559(TxEip1559 {
                                chain_id: 41454,
                                nonce,
                                gas_limit: 21_000,
                                max_fee_per_gas: 1_000,
                                max_priority_fee_per_gas: 0,
                                to: TransactionKind::Call(from_address),
                                value: U256::from(1_000_000_000_000_000u128).into(),
                                access_list: AccessList::default(),
                                input: Vec::default().into(),
                            });

                            let signature =
                                self.root_account.1.sign_transaction(&transaction).unwrap();

                            let signed_transaction =
                                TransactionSigned::from_transaction_and_signature(
                                    transaction,
                                    signature,
                                );

                            if should_trace {
                                trace!("pushing seed tx");
                            }
                            txs.push(signed_transaction);
                            return true;
                        }

                        let Some(nonce) =
                            from_account.get_available_nonce(from_account_state, 6, should_trace)
                        else {
                            if should_trace {
                                trace!("Skipping bc from account has no available nonce");
                            }
                            return true;
                        };

                        let transaction = Transaction::Eip1559(TxEip1559 {
                            chain_id: 41454,
                            nonce,
                            gas_limit: 21_000,
                            max_fee_per_gas: 1_000,
                            max_priority_fee_per_gas: 0,
                            to: TransactionKind::Call(to_address),
                            value: quantity.to_owned().into(),
                            access_list: AccessList::default(),
                            input: Vec::default().into(),
                        });

                        let signature = from_account.sign_transaction(&transaction).unwrap();

                        let signed_transaction = TransactionSigned::from_transaction_and_signature(
                            transaction,
                            signature,
                        );
                        if should_trace {
                            trace!("pushing normal transfer tx");
                        }
                        txs.push(signed_transaction);

                        true
                    })
            }
            EthTxActivityType::Erc20TokenTransfer { .. } => unimplemented!(),
        }

        txs
    }
}

pub fn format_addr(addr: &Address) -> String {
    let mut s = addr.to_string();
    s.truncate(8);
    s
}
