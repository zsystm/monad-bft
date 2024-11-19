use std::{
    sync::{Arc, Condvar, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use itertools::Itertools;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_eth_block_policy::EthBlockPolicy;
use monad_state_backend::{StateBackend, StateBackendError};
use tracing::{error, warn};

pub use self::client::EthTxPoolEventLoopClient;
use self::{
    error::EthTxPoolEventLoopError, event::EthTxPoolEventLoopEvent, state::EthTxPoolEventLoopState,
};

mod client;
mod error;
mod event;
mod state;

const NEW_EVENT_TIMEOUT_MS: u64 = 8;

const PROMOTE_PENDING_INTERVAL_MS: u64 = 32;
const PROMOTE_PENDING_MAX_ADDRESSES: usize = 256;

#[derive(Debug)]
pub struct EthTxPoolEventLoop<SCT, SBT>
where
    SCT: SignatureCollection,
{
    state: Arc<Mutex<EthTxPoolEventLoopState<SCT>>>,
    new_event: Arc<Condvar>,

    state_backend: SBT,
}

impl<SCT, SBT> EthTxPoolEventLoop<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    pub fn start(
        block_policy: &EthBlockPolicy,
        state_backend: impl FnOnce() -> SBT + Send + 'static,
    ) -> EthTxPoolEventLoopClient<SCT> {
        let state = EthTxPoolEventLoopState::new_shared(block_policy);

        let new_event = Arc::new(Condvar::default());

        let handle = {
            let state = state.clone();
            let new_event = new_event.clone();

            std::thread::spawn(move || {
                let event_loop = EthTxPoolEventLoop {
                    state,
                    new_event,
                    state_backend: state_backend(),
                };

                event_loop.run()
            })
        };

        EthTxPoolEventLoopClient::new(handle, state, new_event)
    }

    fn run(self) -> Result<(), EthTxPoolEventLoopError>
    where
        SCT: SignatureCollection,
    {
        let mut promote_pending_timer = Instant::now();

        let mut state = self
            .state
            .lock()
            .map_err(|_| EthTxPoolEventLoopError::PoisonError)?;

        loop {
            state = {
                let (mut new_state, wait) = self
                    .new_event
                    .wait_timeout(state, Duration::from_millis(NEW_EVENT_TIMEOUT_MS))
                    .map_err(|_| EthTxPoolEventLoopError::PoisonError)?;

                if !wait.timed_out() {
                    new_state.process_all_events();
                }

                new_state
            };

            if Instant::now()
                < promote_pending_timer + Duration::from_millis(PROMOTE_PENDING_INTERVAL_MS)
            {
                continue;
            }

            state = self.try_promote_pending(state)?;

            promote_pending_timer = Instant::now();
        }
    }

    fn try_promote_pending(
        &self,
        state: MutexGuard<'_, EthTxPoolEventLoopState<SCT>>,
    ) -> Result<MutexGuard<'_, EthTxPoolEventLoopState<SCT>>, EthTxPoolEventLoopError> {
        let seqnum = state.last_commit_seq_num();

        let pending_addresses = state
            .iter_pending_addresses()
            .take(PROMOTE_PENDING_MAX_ADDRESSES)
            .cloned()
            .collect_vec();

        let (mut state, account_statuses_result) = {
            drop(state);

            let result = self
                .state_backend
                .get_account_statuses(seqnum, pending_addresses.iter());

            (
                self.state
                    .lock()
                    .map_err(|_| EthTxPoolEventLoopError::PoisonError)?,
                result,
            )
        };

        match account_statuses_result {
            Err(StateBackendError::NotAvailableYet) => {
                warn!("txpool state backend error not available yet");
            }
            Err(StateBackendError::NeverAvailable) => {
                error!("txpool state backend error never available");
                return Err(StateBackendError::NeverAvailable.into());
            }
            Ok(accounts) => {
                assert_eq!(
                    pending_addresses.len(),
                    accounts.len(),
                    "accounts is same length as addresses"
                );

                state.insert_tracked(
                    seqnum,
                    pending_addresses.into_iter().zip(accounts.into_iter()),
                );
            }
        }

        Ok(state)
    }
}
