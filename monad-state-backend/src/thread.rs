use std::sync::{mpsc, Arc};

use alloy_primitives::Address;
use monad_eth_types::EthAccount;
use monad_types::{BlockId, Round, SeqNum};
use tracing::warn;

use crate::{StateBackend, StateBackendError};

// Since the StateBackendThreadClient is synchronous, it will only allow one inflight request per
// sync context so a value of 16 allows 16 threads to simulatneously make state backend requests.
const MAX_INFLIGHT_REQUESTS: usize = 16;

enum StateBackendThreadRequest {
    GetAccountStatues {
        block_id: BlockId,
        seq_num: SeqNum,
        round: Round,
        is_finalized: bool,
        addresses: Vec<Address>,
        tx: mpsc::SyncSender<Result<Vec<Option<EthAccount>>, StateBackendError>>,
    },
    RawReadEarliestFinalizedBlock {
        tx: mpsc::SyncSender<Option<SeqNum>>,
    },
    RawReadLatestFinalizedBlock {
        tx: mpsc::SyncSender<Option<SeqNum>>,
    },
    TotalDbLookups {
        tx: mpsc::SyncSender<u64>,
    },
}

#[derive(Clone)]
pub struct StateBackendThreadClient {
    handle: Arc<std::thread::JoinHandle<()>>,
    request_tx: mpsc::SyncSender<StateBackendThreadRequest>,
}

impl StateBackendThreadClient {
    pub fn new<SBT>(state_backend: impl FnOnce() -> SBT + Send + 'static) -> Self
    where
        SBT: StateBackend,
    {
        let (request_tx, request_rx) = mpsc::sync_channel(MAX_INFLIGHT_REQUESTS);

        let handle = Arc::new(std::thread::spawn(move || {
            StateBackendThread::new(state_backend, request_rx).run()
        }));

        Self { handle, request_tx }
    }

    fn send_and_recv_request<T>(
        &self,
        request: impl FnOnce(mpsc::SyncSender<T>) -> StateBackendThreadRequest,
    ) -> T {
        if self.handle.is_finished() {
            panic!("StateBackendThread terminated!");
        }

        let (tx, rx) = mpsc::sync_channel(0);

        self.request_tx
            .send(request(tx))
            .expect("StateBackendThread is alive");

        rx.recv().expect("StateBackendThread sends response")
    }
}

impl StateBackend for StateBackendThreadClient {
    fn get_account_statuses<'a>(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        round: &Round,
        is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        self.send_and_recv_request(|tx| StateBackendThreadRequest::GetAccountStatues {
            block_id: block_id.to_owned(),
            seq_num: seq_num.to_owned(),
            round: round.to_owned(),
            is_finalized,
            addresses: addresses.cloned().collect(),
            tx,
        })
    }

    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum> {
        self.send_and_recv_request(
            |tx| StateBackendThreadRequest::RawReadEarliestFinalizedBlock { tx },
        )
    }

    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum> {
        self.send_and_recv_request(
            |tx| StateBackendThreadRequest::RawReadLatestFinalizedBlock { tx },
        )
    }

    fn total_db_lookups(&self) -> u64 {
        self.send_and_recv_request(|tx| StateBackendThreadRequest::TotalDbLookups { tx })
    }
}

struct StateBackendThread<SBT>
where
    SBT: StateBackend,
{
    state_backend: SBT,
    request_rx: mpsc::Receiver<StateBackendThreadRequest>,
}

impl<SBT> StateBackendThread<SBT>
where
    SBT: StateBackend,
{
    fn new(
        state_backend: impl FnOnce() -> SBT + Send + 'static,
        request_rx: mpsc::Receiver<StateBackendThreadRequest>,
    ) -> Self {
        let state_backend = state_backend();

        Self {
            state_backend,
            request_rx,
        }
    }

    fn run(self) {
        let Self {
            state_backend,
            request_rx,
        } = self;

        for request in request_rx.iter() {
            match request {
                StateBackendThreadRequest::GetAccountStatues {
                    block_id,
                    seq_num,
                    round,
                    is_finalized,
                    addresses,
                    tx,
                } => {
                    tx.send(state_backend.get_account_statuses(
                        &block_id,
                        &seq_num,
                        &round,
                        is_finalized,
                        addresses.iter(),
                    ))
                    .expect("StateBackendThreadClient is alive");
                }
                StateBackendThreadRequest::RawReadEarliestFinalizedBlock { tx } => {
                    tx.send(state_backend.raw_read_earliest_finalized_block())
                        .expect("StateBackendThreadClient is alive");
                }
                StateBackendThreadRequest::RawReadLatestFinalizedBlock { tx } => {
                    tx.send(state_backend.raw_read_latest_finalized_block())
                        .expect("StateBackendThreadClient is alive");
                }
                StateBackendThreadRequest::TotalDbLookups { tx } => {
                    tx.send(state_backend.total_db_lookups())
                        .expect("StateBackendThreadClient is alive");
                }
            }
        }

        warn!("StateBackendThread terminating");
    }
}
