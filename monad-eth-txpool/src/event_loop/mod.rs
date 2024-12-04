use std::{
    sync::{Arc, Condvar},
    time::Duration,
};

use monad_consensus_types::signature_collection::SignatureCollection;
use monad_eth_block_policy::EthBlockPolicy;
use state::SharedEthTxPoolEventLoopState;

pub use self::client::EthTxPoolEventLoopClient;
// TODO(andr-dev): Make PendingEthTxMap private after async promote pending
pub use self::pending::PendingEthTxMap;
use self::{
    error::EthTxPoolEventLoopError, event::EthTxPoolEventLoopEvent, state::EthTxPoolEventLoopState,
};

mod client;
mod error;
mod event;
mod pending;
mod state;

const EVENTS_CAPACITY: usize = 1024;
const NEW_EVENT_TIMEOUT_US: u64 = 8;

#[derive(Debug)]
pub struct EthTxPoolEventLoop<SCT>
where
    SCT: SignatureCollection,
{
    state: SharedEthTxPoolEventLoopState<SCT>,
    new_event: Arc<Condvar>,
}

impl<SCT> EthTxPoolEventLoop<SCT>
where
    SCT: SignatureCollection,
{
    pub fn start(block_policy: &EthBlockPolicy) -> EthTxPoolEventLoopClient<SCT> {
        let (events_tx, events_rx) = rtrb::RingBuffer::new(EVENTS_CAPACITY);

        let state = EthTxPoolEventLoopState::new_shared(block_policy, events_rx);

        let new_event = Arc::new(Condvar::default());

        let handle = {
            let state = state.clone();
            let new_event = new_event.clone();

            std::thread::spawn(move || {
                let event_loop = EthTxPoolEventLoop { state, new_event };

                event_loop.run()
            })
        };

        EthTxPoolEventLoopClient::new(handle, state, events_tx, new_event)
    }

    fn run(self) -> Result<(), EthTxPoolEventLoopError>
    where
        SCT: SignatureCollection,
    {
        let mut state = self
            .state
            .lock()
            .map_err(|_| EthTxPoolEventLoopError::PoisonError)?;

        loop {
            state = {
                let (mut new_state, wait) = self
                    .new_event
                    .wait_timeout(state, Duration::from_micros(NEW_EVENT_TIMEOUT_US))
                    .map_err(|_| EthTxPoolEventLoopError::PoisonError)?;

                // TODO(andr-dev): Add assertions about event (non-/)emptyness

                if !wait.timed_out() {
                    new_state.process_all_events();
                }

                new_state
            };

            // TODO(andr-dev): Promote pending nonces and fetch account balances
        }
    }
}
