use std::collections::HashMap;

use rand::{
    rngs::{SmallRng, StdRng, ThreadRng}, seq::SliceRandom, Rng, SeedableRng
};
use reth_primitives::Address;

use super::{account::EthAccount, config::EthTxAddressPoolConfig};
use crate::{account::PrivateKey, state::ChainStateView};

#[derive(Debug)]
pub struct AccountPool {
    accounts: HashMap<Address, EthAccount>,
    pub from: Vec<Address>,
    to: Vec<Address>,
}

impl AccountPool {
    pub async fn new_with_config(
        from_config: EthTxAddressPoolConfig,
        to_config: EthTxAddressPoolConfig,
        chain_state: &ChainStateView,
    ) -> Self {
        let mut accounts = HashMap::default();

        let mut from = Vec::default();
        let mut to = Vec::default();

        Self::register_accounts(chain_state, &mut accounts, &mut from, from_config).await;
        Self::register_accounts(chain_state, &mut accounts, &mut to, to_config).await;

        Self { accounts, from, to }
    }

    async fn register_accounts(
        chain_state: &ChainStateView,
        accounts: &mut HashMap<Address, EthAccount>,
        addresses: &mut Vec<Address>,
        config: EthTxAddressPoolConfig,
    ) {
        let list = match config {
            EthTxAddressPoolConfig::Single { private_key } => {
                let (address, account) = PrivateKey::new(private_key);

                vec![(address, account)]
            }
            EthTxAddressPoolConfig::RandomSeeded { seed, count } => {
                let mut rng = StdRng::seed_from_u64(seed);

                (0..count)
                    .map(|_| PrivateKey::new_with_random(&mut rng))
                    .collect()
            }
        };

        for (address, account) in list {
            addresses.push(address);

            if accounts.insert(address, EthAccount::new(account)).is_none() {
                chain_state.add_new_account(address).await;
            }
        }
    }

    pub fn iter_random(
        &mut self,
        limit: usize,
        mut f: impl FnMut(Address, &mut EthAccount, Address) -> bool,
    ) {
        let mut from_generator = Self::generator(&self.from);
        let mut to_generator = Self::generator(&self.to);

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

    fn generator<'a>(list: &'a [Address]) -> Box<dyn FnMut() -> Option<Address> + 'a> {
        let mut rng = ThreadRng::default();
        let mut idxs = (0..list.len()).collect::<Vec<_>>();

        Box::new(move || {
            if list.len() == 1 {
                return Some(list[0]);
            }

            if idxs.is_empty() {
                return None;
            }

            let idx_idx = rng.gen_range(0..idxs.len());
            let last_idx_idx = idxs.len() - 1;

            idxs.swap(idx_idx, last_idx_idx);

            let idx = idxs.pop().expect("last element exists");

            Some(list[idx])
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
