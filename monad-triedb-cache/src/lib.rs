use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use alloy_primitives::Address;
use itertools::Itertools;
use monad_eth_types::{EthAccount, EthHeader};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::{BlockId, DropTimer, SeqNum};
use tracing::warn;

#[derive(Debug)]
struct BlockCache {
    seq_num: SeqNum,
    accounts: BTreeMap<Address, Option<EthAccount>>,
    execution_result: Option<EthHeader>,
}

#[derive(Debug)]
pub struct StateBackendCache<SBT> {
    // used so that StateBackendCache can maintain a logically immutable interface
    cache: Arc<Mutex<HashMap<BlockId, BlockCache>>>,
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
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
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
        let cache_misses: Vec<_> = match cache.get(block_id) {
            None => unique_addresses.collect(),
            Some(block_cache) => unique_addresses
                .filter(|&address| !block_cache.accounts.contains_key(address))
                .collect(),
        };

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
                self.state_backend.get_account_statuses(
                    block_id,
                    seq_num,
                    is_finalized,
                    cache_misses.iter().copied(),
                )?
            };
            cache
                .entry(*block_id)
                .or_insert_with(|| BlockCache {
                    seq_num: *seq_num,
                    accounts: Default::default(),
                    execution_result: None,
                })
                .accounts
                .extend(
                    cache_misses
                        .iter()
                        .map(|&&address| address)
                        .zip_eq(cache_misses_data),
                )
        }

        let block_cache = cache
            .get(block_id)
            .expect("cache must be populated... we asserted nonzero addresses at the start");

        let accounts_data = addresses
            .iter()
            .map(|&address| {
                block_cache
                    .accounts
                    .get(address)
                    .expect("cache was hydrated")
            })
            .cloned()
            .collect();

        let last_finalized_block = self
            .raw_read_latest_finalized_block()
            .unwrap_or(SeqNum::MAX);

        cache.retain(|_, block| block.seq_num + self.execution_delay >= last_finalized_block);

        Ok(accounts_data)
    }

    fn get_execution_result(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
    ) -> Result<EthHeader, StateBackendError> {
        let mut cache = self.cache.lock().unwrap();

        if let Some(block_cache) = cache.get(block_id) {
            if let Some(execution_result) = &block_cache.execution_result {
                return Ok(execution_result.clone());
            }
        }

        let execution_result =
            self.state_backend
                .get_execution_result(block_id, seq_num, is_finalized)?;

        cache
            .entry(*block_id)
            .or_insert_with(|| BlockCache {
                seq_num: *seq_num,
                accounts: Default::default(),
                execution_result: None,
            })
            .execution_result = Some(execution_result.clone());

        Ok(execution_result)
    }

    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum> {
        self.state_backend.raw_read_earliest_finalized_block()
    }

    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum> {
        self.state_backend.raw_read_latest_finalized_block()
    }

    fn total_db_lookups(&self) -> u64 {
        self.state_backend.total_db_lookups()
    }
}
