use std::collections::HashMap;

use rand::{
    rngs::{SmallRng, StdRng, ThreadRng},
    seq::SliceRandom,
    Rng, SeedableRng,
};
use reth_primitives::Address;
use tokio::sync::mpsc::{self, Receiver};
use tracing::{debug, info, trace, warn};

use super::{account::EthAccount, config::EthTxAddressPoolConfig};
use crate::{private_key::PrivateKey, generator::format_addr, state::ChainStateView};

#[derive(Debug)]
pub struct AccountPool {
    accounts: HashMap<Address, EthAccount>,
    pub from: Vec<Address>,
    to: Vec<Address>,

    async_from_registration_rx: Receiver<Vec<(Address, PrivateKey)>>,
    async_to_registration_rx: Receiver<Vec<(Address, PrivateKey)>>,
}

impl AccountPool {
    pub async fn new_with_config(
        from_config: EthTxAddressPoolConfig,
        to_config: EthTxAddressPoolConfig,
        chain_state: &ChainStateView,
    ) -> Self {
        let mut accounts = HashMap::default();

        let (from, async_from_registration_rx) =
            Self::register_accounts(chain_state, &mut accounts, from_config).await;
        let (to, async_to_registration_rx) =
            Self::register_accounts(chain_state, &mut accounts, to_config).await;

        Self {
            accounts,
            from,
            to,
            async_from_registration_rx,
            async_to_registration_rx,
        }
    }

    async fn register_accounts(
        chain_state: &ChainStateView,
        accounts: &mut HashMap<Address, EthAccount>,
        config: EthTxAddressPoolConfig,
    ) -> (Vec<Address>, mpsc::Receiver<Vec<(Address, PrivateKey)>>) {
        info!("Registering accounts in EthTxAddressPoolConfig...");

        let mut addresses;
        let mut list: Box<dyn Iterator<Item = (Address, PrivateKey)> + Send> = match config {
            EthTxAddressPoolConfig::Single { private_key } => {
                addresses = Vec::with_capacity(1);
                Box::new([PrivateKey::new(private_key)].into_iter())
            }
            EthTxAddressPoolConfig::RandomSeeded { seed, count } => {
                let mut rng = SmallRng::seed_from_u64(seed);
                addresses = Vec::with_capacity(count);
                Box::new((0..count).map(move |_| PrivateKey::new_with_random(&mut rng)))
            }
        };

        for (i, (address, account)) in list.by_ref().take(100_000).enumerate() {
            addresses.push(address);
            if accounts.insert(address, EthAccount::new(account)).is_none() {
                if i < 10 {
                trace!(
                    addr = address.to_string(),
                    "Registering account with chain state and inserting in AccountPool (logging first 10..)"
                );
                }
                chain_state.add_new_account(address).await;
            } else {
                warn!("Detected duplicate address (this is expected if to and from seeds are the same)");
            }
        }

        let mut list = list.peekable();
        let (async_registration_sender, async_registration_rx) = mpsc::channel(100);
        if list.peek().is_some() {
            info!("Too many accounts in EthTxAddressPoolConfig... moving to async");
            tokio::task::spawn_blocking(move || {
                info!("Spawned async registration worker...");
                loop {
                    let mut results = Vec::with_capacity(100_000);
                    results.extend(list.by_ref().take(100_000));

                    info!(
                        batch_size = results.len(),
                        "Async registration batch finished"
                    );

                    // we don't care if channel is closed
                    let _ = async_registration_sender.blocking_send(results);

                    if list.peek().is_none() {
                        info!("Done registering accounts in EthTxAddressPoolConfig...");
                        break;
                    }
                }
            });
        } else {
            info!("Done registering accounts in EthTxAddressPoolConfig...");
        }

        (addresses, async_registration_rx)
    }

    pub(super) async fn process_async_registrations(&mut self, chain_state: &ChainStateView) {
        // to
        if let Ok(new) = self.async_to_registration_rx.try_recv() {
            info!(
                num_to_accts = new.len(),
                "Processing batch of async registrations.."
            );
            for (address, account) in new {
                self.to.push(address);
                let insert_opt = self.accounts.insert(address, EthAccount::new(account));
                match insert_opt {
                    Some(_) => warn!("Detected duplicate address (this is expected if to and from seeds are the same)"),
                    None =>chain_state.add_new_account(address).await, 
                }
            }
        }
        // from
        if let Ok(new) = self.async_from_registration_rx.try_recv() {
            info!(
                num_from_accts = new.len(),
                "Processing batch of async registrations.."
            );
            for (address, account) in new {
                self.from.push(address);
                let insert_opt = self.accounts.insert(address, EthAccount::new(account));
                match insert_opt {
                    Some(_) => warn!("Detected duplicate address (this is expected if to and from seeds are the same)"),
                    None =>chain_state.add_new_account(address).await, 
                }
            }
        }
    }

    pub fn iter_random(
        &mut self,
        limit: usize,
        mut f: impl FnMut(Address, &mut EthAccount, Address) -> bool,
    ) {
        let mut from_generator = Self::generator(&self.from, limit);
        let mut to_generator = Self::generator(&self.to, limit);

        for _ in 0..limit {
            let Some(from) = from_generator() else {
                return;
            };

            let Some(to) = to_generator() else {
                return;
            };

            let from_account = self.accounts.get_mut(&from).expect("from account exists");

            if !f(from, from_account, to) {
                break;
            }
        }
    }

    fn generator<'a>(
        list: &'a [Address],
        limit: usize,
    ) -> Box<dyn FnMut() -> Option<Address> + 'a> {
        let mut rng = ThreadRng::default();
        let mut elems: _ = list.choose_multiple(&mut rng, list.len().min(limit).min(10000));

        Box::new(move || {
            match elems.next() {
                Some(e) => Some(e.clone()),
                None => {
                    // trace!("None branch");
                    elems = list.choose_multiple(&mut rng, list.len().min(10000));
                    elems.next().cloned()
                }
            }
        })
    }

    // pub fn iter_random<'a>(
    //     &'a mut self,
    //     limit: usize,
    // ) -> (RandomAccountIter<'a>, &'a mut HashMap<Address, EthAccount>) {
    //     let mut to_idxs: Vec<_> = (0..self.to.len()).collect();
    //     let mut from_idxs: Vec<_> = (0..self.from.len()).collect();
    //     let mut rng = SmallRng::from_entropy();
    //     to_idxs.shuffle(&mut rng);
    //     from_idxs.shuffle(&mut rng);

    //     (
    //         RandomAccountIter {
    //             to_idxs: to_idxs.into_iter(),
    //             from_idxs: from_idxs.into_iter(),
    //             to: &self.to,
    //             from: &self.from,
    //             limit,
    //             // accounts: &mut self.accounts,
    //         },
    //         &mut self.accounts,
    //     )
    // }
}

// pub struct RandomAccountIter<'a> {
//     to_idxs: std::vec::IntoIter<usize>,
//     from_idxs: std::vec::IntoIter<usize>,
//     to: &'a [Address],
//     from: &'a [Address],
//     limit: usize,
// }

// impl<'a> Iterator for RandomAccountIter<'a> {
//     type Item = (Address, Address);

//     fn next(&mut self) -> Option<Self::Item> {
//         if self.limit == 0 {
//             return None;
//         }
//         self.limit -= 1;
//         let to = self.to[self.to_idxs.next()?];
//         let from = self.from[self.from_idxs.next()?];
//         Some((from, to))
//     }
// }
