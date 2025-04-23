use std::sync::{mpsc, Arc};

use alloy_primitives::Address;
use monad_eth_types::EthAccount;
use monad_types::{BlockId, Round, SeqNum};

use crate::{StateBackend, StateBackendError};

pub enum SyncStateBackendWrapperRequest {
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
pub struct SyncStateBackendWrapper {
    handle: Arc<std::thread::JoinHandle<()>>,
    request_tx: mpsc::SyncSender<SyncStateBackendWrapperRequest>,
}

impl SyncStateBackendWrapper {
    pub fn new<SBT>(state_backend: impl FnOnce() -> SBT + Send + 'static) -> Self
    where
        SBT: StateBackend,
    {
        let (request_tx, request_rx) = mpsc::sync_channel(64);

        let handle = Arc::new(std::thread::spawn(move || {
            SyncStateBackendThread::new(state_backend, request_rx).run()
        }));

        Self { handle, request_tx }
    }

    fn send_and_recv_request<T>(
        &self,
        request: impl FnOnce(mpsc::SyncSender<T>) -> SyncStateBackendWrapperRequest,
    ) -> T {
        if self.handle.is_finished() {
            panic!("SyncStateBackendThread terminated!");
        }

        let (tx, rx) = mpsc::sync_channel(1);

        self.request_tx
            .send(request(tx))
            .expect("SyncStateBackendThread is alive");

        rx.recv().expect("SyncStateBackendThread sends response")
    }
}

impl StateBackend for SyncStateBackendWrapper {
    fn get_account_statuses<'a>(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        round: &Round,
        is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        self.send_and_recv_request(|tx| SyncStateBackendWrapperRequest::GetAccountStatues {
            block_id: block_id.to_owned(),
            seq_num: seq_num.to_owned(),
            round: round.to_owned(),
            is_finalized,
            addresses: addresses.cloned().collect(),
            tx,
        })
    }

    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum> {
        self.send_and_recv_request(|tx| {
            SyncStateBackendWrapperRequest::RawReadEarliestFinalizedBlock { tx }
        })
    }

    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum> {
        self.send_and_recv_request(|tx| {
            SyncStateBackendWrapperRequest::RawReadLatestFinalizedBlock { tx }
        })
    }

    fn total_db_lookups(&self) -> u64 {
        self.send_and_recv_request(|tx| SyncStateBackendWrapperRequest::TotalDbLookups { tx })
    }
}

struct SyncStateBackendThread<SBT>
where
    SBT: StateBackend,
{
    state_backend: SBT,
    request_rx: mpsc::Receiver<SyncStateBackendWrapperRequest>,
}

impl<SBT> SyncStateBackendThread<SBT>
where
    SBT: StateBackend,
{
    fn new(
        state_backend: impl FnOnce() -> SBT + Send + 'static,
        request_rx: mpsc::Receiver<SyncStateBackendWrapperRequest>,
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
                SyncStateBackendWrapperRequest::GetAccountStatues {
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
                    .expect("SyncStateBackendWrapper is alive");
                }
                SyncStateBackendWrapperRequest::RawReadEarliestFinalizedBlock { tx } => {
                    tx.send(state_backend.raw_read_earliest_finalized_block())
                        .expect("SyncStateBackendWrapper is alive");
                }
                SyncStateBackendWrapperRequest::RawReadLatestFinalizedBlock { tx } => {
                    tx.send(state_backend.raw_read_latest_finalized_block())
                        .expect("SyncStateBackendWrapper is alive");
                }
                SyncStateBackendWrapperRequest::TotalDbLookups { tx } => {
                    tx.send(state_backend.total_db_lookups())
                        .expect("SyncStateBackendWrapper is alive");
                }
            }
        }
    }
}
