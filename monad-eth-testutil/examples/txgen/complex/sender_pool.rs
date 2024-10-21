use eyre::ensure;
use futures::{
    future::{join, join_all},
    stream::FuturesUnordered,
    StreamExt,
};

use rand::{rngs::SmallRng, SeedableRng};
use serde::Deserialize;
use tokio::time::{interval, sleep};

use crate::{
    prelude::*,
    complex::run::Balances,
    shared::{erc20::ERC20, json_rpc::JsonRpc},
};

/// Pool of sender accounts
/// - Provides Iterator of senders with ready nonces
/// - Periodically refreshes senders nonces + balances
pub struct SenderPool {
    accts: AcctMap,
    senders: Arc<Vec<Address>>,
}

impl SenderPool {
    pub async fn new(
        pool_config: SenderPoolConfig,
        erc20: Option<ERC20>,
        client: ReqwestClient,
        accts: AcctMap,
    ) -> Result<Self> {
        let mut senders = Vec::with_capacity(pool_config.accts.count());
        let accounts = pool_config.accts.key_iter();

        for (addr, key) in accounts {
            senders.push(addr);
            accts.insert(
                addr,
                GenAccount {
                    addr,
                    key,
                    committed_balance: Balances::default(),
                    last_committed_tx: (None, 0),
                    in_flight: VecDeque::default(),
                },
            );
        }

        // ensure accounts are refreshed before returning
        let futs = FuturesUnordered::new();
        for (i, batch) in senders.chunks(500).enumerate() {
            // don't flood rpc
            sleep(Duration::from_millis(i as u64 * 10)).await;

            futs.push(refresh_batch(
                client.clone(),
                batch,
                Arc::clone(&accts),
                erc20,
            ));
        }
        futs.count().await;

        Ok(Self {
            accts,
            senders: Arc::new(senders),
        })
    }

    // async fn refresh_worker(self: Arc<Self>) {
    //     let mut interval = interval(Duration::from_secs(1))

    // }
}

pub async fn refresh_batch(
    client: ReqwestClient,
    addrs: &[Address],
    accts: AcctMap,
    erc20: Option<ERC20>,
) -> Result<()> {
    ensure!(
        addrs.len() <= 500,
        "Must call refresh batch with less than 500 addrs"
    );
    let str_addrs = addrs.iter().map(|a| a.to_string()).collect::<Vec<_>>();

    let erc20_fut = {
        let client = client.clone();
        let addrs = &addrs;
        async move {
            match erc20 {
                Some(erc20) => Some(client.batch_get_erc20_balance(&addrs, erc20).await),
                None => None,
            }
        }
    };
    let (erc20_res, native_bals, nonces) = tokio::join!(
        erc20_fut,
        client.batch_get_balance(&str_addrs),
        client.batch_get_transaction_count(&str_addrs),
    );

    let native_bals = native_bals?;
    let nonces = nonces?;
    let erc20_bals = match erc20_res {
        Some(res) => Some(res?),
        None => None,
    };

    for (i, addr) in addrs.iter().enumerate() {
        let mut acct = accts.get_mut(addr).context("missing acct")?;

        // let maybe_erc20 = erc20_bals.as_ref().map(|b: _| b[i]);
        // acct.commit_refresh(nonces[i]?, native_bals[i]?, maybe_erc20);
    }

    Ok(())
}

/* Config */

#[derive(Deserialize, Debug)]
pub struct SenderPoolConfig {
    #[serde(flatten)]
    accts: AccountSourceConfig,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AccountSourceConfig {
    Single { private_key: String },
    RandomSeeded { seed: u64, count: usize },
}

impl AccountSourceConfig {
    pub fn count(&self) -> usize {
        match self {
            AccountSourceConfig::Single { .. } => 1,
            AccountSourceConfig::RandomSeeded { count, .. } => *count,
        }
    }

    pub fn key_iter(self) -> Box<dyn Iterator<Item = (Address, PrivateKey)> + Send + Sync> {
        match self {
            Self::Single { private_key } => Box::new([PrivateKey::new(private_key)].into_iter()),
            Self::RandomSeeded { seed, count } => {
                let mut rng = SmallRng::seed_from_u64(seed);
                Box::new((0..count).map(move |_| PrivateKey::new_with_random(&mut rng)))
            }
        }
    }
}
