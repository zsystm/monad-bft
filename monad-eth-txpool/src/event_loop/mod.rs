use std::sync::{Arc, Condvar, MutexGuard};

use monad_consensus_types::signature_collection::SignatureCollection;
use monad_eth_block_policy::EthBlockPolicy;

pub use self::client::EthTxPoolEventLoopClient;
use self::{
    error::EthTxPoolEventLoopError, event::EthTxPoolEventLoopEvent, state::EthTxPoolEventLoopState,
};

mod client;
mod error;
mod event;
mod state;

#[derive(Debug)]
pub struct EthTxPoolEventLoop<'a, SCT>
where
    SCT: SignatureCollection,
{
    state: MutexGuard<'a, EthTxPoolEventLoopState<SCT>>,
    new_event: Arc<Condvar>,
}

impl<'a, SCT> EthTxPoolEventLoop<'a, SCT>
where
    SCT: SignatureCollection,
{
    pub fn start(block_policy: &EthBlockPolicy) -> EthTxPoolEventLoopClient<SCT> {
        let state = EthTxPoolEventLoopState::new_shared(block_policy);

        let new_event = Arc::new(Condvar::default());

        let handle = {
            let state = state.clone();
            let new_event = new_event.clone();

            std::thread::spawn(move || {
                let event_loop = EthTxPoolEventLoop {
                    state: state.lock().expect("state lock is not poisoned"),
                    new_event,
                };

                event_loop.run()
            })
        };

        EthTxPoolEventLoopClient::new(handle, state, new_event)
    }

    fn run(mut self) -> Result<(), EthTxPoolEventLoopError>
    where
        SCT: SignatureCollection,
    {
        loop {
            self.state = self
                .new_event
                .wait(self.state)
                .map_err(|_| EthTxPoolEventLoopError::PoisonError)?;

            self.state.process_all_events();
        }
    }
}
