use std::{
    collections::BTreeMap,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use alloy_consensus::TxEnvelope;
use alloy_primitives::TxHash;
use futures::StreamExt;
use monad_eth_txpool_ipc::EthTxPoolIpcStream;
use monad_eth_txpool_types::{EthTxPoolEvent, EthTxPoolEventAction, EthTxPoolSnapshot};
use pin_project::pin_project;
use tokio::{
    net::UnixListener,
    time::{self, Sleep},
};
use tracing::{info, warn};

pub use self::config::EthTxPoolIpcConfig;

mod config;

const MAX_BATCH_LEN: usize = 128;
const BATCH_TIMER_INTERVAL_MS: u64 = 10;

#[pin_project(project = EthTxPoolIpcServerProjected)]
pub struct EthTxPoolIpcServer {
    #[pin]
    listener: UnixListener,

    connections: Vec<EthTxPoolIpcStream>,

    batch: Vec<TxEnvelope>,
    #[pin]
    batch_timer: Sleep,
}

impl EthTxPoolIpcServer {
    pub fn new(
        EthTxPoolIpcConfig {
            bind_path,
            tx_batch_size: _,
            max_queued_batches,
            queued_batches_watermark,
        }: EthTxPoolIpcConfig,
    ) -> Result<Self, io::Error> {
        assert!(queued_batches_watermark <= max_queued_batches);

        let listener = UnixListener::bind(bind_path)?;

        Ok(Self {
            listener,

            connections: Vec::default(),

            batch: Vec::default(),
            batch_timer: time::sleep(Duration::ZERO),
        })
    }

    pub fn broadcast_tx_events(
        self: Pin<&mut Self>,
        events: BTreeMap<TxHash, EthTxPoolEventAction>,
    ) {
        if events.is_empty() {
            return;
        }

        let events: Vec<EthTxPoolEvent> = events
            .into_iter()
            .map(|(tx_hash, action)| EthTxPoolEvent { tx_hash, action })
            .collect();

        self.project()
            .connections
            .retain(|stream| match stream.send_tx_events(events.clone()) {
                Ok(()) => true,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    warn!("dropping ipc stream, reason: channel full!");
                    false
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    info!("dropping ipc stream, reason: channel closed!");
                    false
                }
            });
    }

    pub fn poll_txs(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        generate_snapshot: impl Fn() -> EthTxPoolSnapshot,
    ) -> Poll<Vec<TxEnvelope>> {
        let EthTxPoolIpcServerProjected {
            listener,

            connections,

            batch,
            mut batch_timer,
        } = self.project();

        while let Poll::Ready(result) = listener.poll_accept(cx) {
            match result {
                Err(error) => {
                    warn!("listener poll accept error={error:?}");
                    continue;
                }
                Ok((stream, _)) => {
                    connections.push(EthTxPoolIpcStream::new(stream, generate_snapshot()));
                }
            }
        }

        let batch_was_empty = batch.is_empty();

        connections.retain_mut(|stream| {
            loop {
                if batch.len() >= MAX_BATCH_LEN {
                    break;
                }

                let Poll::Ready(result) = stream.poll_next_unpin(cx) else {
                    break;
                };

                let Some(tx) = result else {
                    return false;
                };

                batch.push(tx);
            }

            true
        });

        if batch.is_empty() {
            return Poll::Pending;
        }

        if batch_was_empty {
            batch_timer.set(time::sleep(Duration::from_millis(BATCH_TIMER_INTERVAL_MS)));
        }

        if batch.len() >= MAX_BATCH_LEN || batch_timer.as_mut().poll(cx).is_ready() {
            return Poll::Ready(std::mem::take(batch));
        }

        Poll::Pending
    }
}
