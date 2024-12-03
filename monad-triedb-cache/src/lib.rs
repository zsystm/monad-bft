use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use itertools::Itertools;
use monad_eth_types::{EthAccount, EthAddress};
use monad_metrics::METRICS;
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::{DropTimer, SeqNum};
use tracing::{trace, warn};

#[derive(Debug)]
pub struct StateBackendCache<SBT> {
    // used so that StateBackendCache can maintain a logically immutable interface
    cache: Arc<Mutex<BTreeMap<SeqNum, BTreeMap<EthAddress, Option<EthAccount>>>>>,
    state_backend: SBT,
    execution_delay: SeqNum,
}

impl<SBT> StateBackendCache<SBT>
where
    SBT: StateBackend,
{
    pub fn new(state_backend: SBT, execution_delay: SeqNum) -> Self {
        Self {
            cache: Default::default(),
            state_backend,
            execution_delay,
        }
    }
}

impl<SBT> StateBackend for StateBackendCache<SBT>
where
    SBT: StateBackend,
{
    fn get_account_statuses<'a>(
        &self,
        block: SeqNum,
        addresses: impl Iterator<Item = &'a EthAddress>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        let addresses = addresses.collect_vec();
        if addresses.is_empty() {
            return Ok(Vec::new());
        }

        let mut cache = self.cache.lock().unwrap();

        // TODO consider removing this uniqueness filter... the callers we have so far already only
        // pass in a unique set of accounts
        let unique_addresses = addresses.iter().unique().copied();
        // find accounts that are missing from cache
        let cache_misses: Vec<_> = match cache.get(&block) {
            None => unique_addresses.collect(),
            Some(block_cache) => unique_addresses
                .filter(|address| !block_cache.contains_key(address))
                .collect(),
        };

        let unique_addresses_cnt = addresses.iter().unique().count();
        if !cache_misses.is_empty() {
            // hydrate cache with missing accounts
            let cache_misses_data = {
                let _timer = DropTimer::start(Duration::from_millis(10), |elapsed| {
                    warn!(
                        ?elapsed,
                        lookups = cache_misses.len(),
                        "long get_account_statuses"
                    )
                });
                self.state_backend
                    .get_account_statuses(block, cache_misses.iter().copied())?
            };
            cache.entry(block).or_default().extend(
                cache_misses
                    .iter()
                    .map(|&&address| address)
                    .zip_eq(cache_misses_data),
            )
        }

        if let Ok(mut global_metrics) = METRICS.try_write() {
            let metrics = global_metrics.metrics();

            let cur_cache_misses_cnt = cache_misses.len();

            let cache_misses_cnt = metrics
                .backendcache_events
                .cache_misses
                .checked_add(cur_cache_misses_cnt.try_into().unwrap());

            let cache_hit_cnt = metrics.backendcache_events.cache_hits.checked_add(
                (unique_addresses_cnt - cur_cache_misses_cnt)
                    .try_into()
                    .unwrap(),
            );
            if cache_hit_cnt.is_none() || cache_misses_cnt.is_none() {
                // overflow reset counters
                metrics.backendcache_events.cache_hits = 0;
                metrics.backendcache_events.cache_misses = 0;
                trace!("Updated metrics cache_hits or cache_misses overflow");
            } else {
                metrics.backendcache_events.cache_hits = cache_hit_cnt.unwrap();
                metrics.backendcache_events.cache_misses = cache_misses_cnt.unwrap();
                trace!(
                    "Updated metrics cache_hits: {:?} , cache_misses: {:?}",
                    cache_hit_cnt.unwrap(),
                    cache_misses_cnt.unwrap()
                );
            }
        }

        let block_cache = cache
            .get(&block)
            .expect("cache must be populated... we asserted nonzero addresses at the start");

        let accounts_data = addresses
            .iter()
            .map(|&address| block_cache.get(address).expect("cache was hydrated"))
            .cloned()
            .collect();

        if cache.len() > self.execution_delay.0 as usize * 2 {
            let (evicted, _) = cache.pop_first().expect("nonempty");
            if evicted == block {
                let (latest, _) = cache.last_key_value().expect("nonempty");
                tracing::warn!(
                    ?evicted,
                    ?latest,
                    "unexpected cache thrashing? only expect queries on the 2*delay latest blocks"
                );
            }
        }

        Ok(accounts_data)
    }

    fn raw_read_account(&self, block: SeqNum, address: &EthAddress) -> Option<EthAccount> {
        self.state_backend.raw_read_account(block, address)
    }

    fn raw_read_earliest_block(&self) -> SeqNum {
        self.state_backend.raw_read_earliest_block()
    }

    fn raw_read_latest_block(&self) -> SeqNum {
        self.state_backend.raw_read_latest_block()
    }
}
