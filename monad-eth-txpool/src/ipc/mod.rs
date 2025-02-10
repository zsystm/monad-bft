use std::{io, pin::Pin, task::Poll};

use alloy_consensus::TxEnvelope;
use alloy_rlp::Decodable;
use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt};
use tokio::{
    net::{unix::SocketAddr, UnixListener, UnixStream},
    sync::mpsc,
    time::{Duration, Instant},
};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tracing::{debug, error, trace, warn};

pub use self::config::EthTxPoolIpcConfig;

mod config;

pub struct EthTxPoolIpc {
    handle: tokio::task::JoinHandle<io::Result<()>>,

    tx_batch_receiver: mpsc::Receiver<Vec<Bytes>>,
}

impl EthTxPoolIpc {
    pub fn new(
        EthTxPoolIpcConfig {
            bind_path,
            tx_batch_size,
            max_queued_batches,
            queued_batches_watermark,
        }: EthTxPoolIpcConfig,
    ) -> Result<Self, io::Error> {
        assert!(queued_batches_watermark <= max_queued_batches);

        let (tx_batch_sender, tx_batch_receiver) = mpsc::channel(max_queued_batches);

        let listener = UnixListener::bind(bind_path)?;

        Ok(Self {
            handle: tokio::spawn(Self::run(
                listener,
                tx_batch_sender,
                tx_batch_size,
                queued_batches_watermark,
            )),

            tx_batch_receiver,
        })
    }

    async fn run(
        listener: UnixListener,
        tx_batch_sender: mpsc::Sender<Vec<Bytes>>,
        tx_batch_size: usize,
        queued_batches_watermark: usize,
    ) -> io::Result<()> {
        loop {
            match listener.accept().await {
                Ok((stream, sockaddr)) => {
                    Self::handle_connection(
                        stream,
                        sockaddr,
                        tx_batch_sender.clone(),
                        tx_batch_size,
                        queued_batches_watermark,
                    );
                }
                Err(error) => {
                    warn!("listener poll accept error={:?}", error);
                }
            }
        }
    }

    fn handle_connection(
        stream: UnixStream,
        sockaddr: SocketAddr,
        tx_batch_sender: mpsc::Sender<Vec<Bytes>>,
        tx_batch_size: usize,
        queued_batches_watermark: usize,
    ) {
        let mut reader = FramedRead::new(stream, LengthDelimitedCodec::default());

        let send_batch = move |tx: &mut Vec<Bytes>| {
            if tx.is_empty() {
                return;
            }
            match tx_batch_sender.try_send(tx.to_vec()) {
                Ok(()) => {
                    trace!("bytes received from IPC and sent to channel");
                    let capacity = tx_batch_sender.capacity();
                    let num_queued = tx_batch_sender.max_capacity() - capacity;
                    if num_queued > queued_batches_watermark {
                        warn!(
                            queued_batches_watermark,
                            num_queued,
                            max_capacity = tx_batch_sender.max_capacity(),
                            "transaction IPC recv channel exceeded watermark, are transactions being ingested fast enough?"
                        );
                    }
                }
                Err(mpsc::error::TrySendError::Full(dropped_batch)) => {
                    error!(
                        dropped_batch_len = dropped_batch.len(),
                        max_capacity = tx_batch_sender.max_capacity(),
                        "transaction IPC recv channel full, dropping batch"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    warn!("failed to send, channel closed")
                }
            }
            tx.clear();
        };

        tokio::spawn(async move {
            let mut txns = Vec::with_capacity(tx_batch_size);
            let mut batch_timeout = ArmedSleep::new();

            loop {
                tokio::select! {
                    read = reader.next() => {
                        match read {
                            Some(Ok(bytes)) => {
                                let bytes = bytes.freeze();
                                if !validate_ethtx(&mut bytes.as_ref()) {
                                    break;
                                }

                                txns.push(bytes);
                                batch_timeout.arm();
                                if txns.len() >= tx_batch_size {
                                    send_batch(&mut txns);
                                    batch_timeout.reset();
                                }
                            }
                            Some(Err(err)) => {
                                warn!("framed reader error err={:?}", err);
                                break;
                            }
                            None => {
                                debug!("done reading");
                                break;
                            }
                        }
                    }
                    () = &mut batch_timeout.sleep => {
                        send_batch(&mut txns);
                        batch_timeout.reset();
                    }
                }
            }

            send_batch(&mut txns);
        });
    }
}

fn validate_ethtx(bytes: &mut &[u8]) -> bool {
    match TxEnvelope::decode(bytes) {
        Ok(_) => true,
        Err(err) => {
            warn!("tx decoder error error={:?}", err);
            false
        }
    }
}

impl Stream for EthTxPoolIpc
where
    Self: Unpin,
{
    type Item = Vec<Bytes>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(result) = self.handle.poll_unpin(cx) {
            panic!("EthTxPoolIpc crashed!\nerror: {result:#?}");
        }

        self.tx_batch_receiver.poll_recv(cx)
    }
}

struct ArmedSleep {
    sleep: Pin<Box<tokio::time::Sleep>>,
    armed: bool,
}

impl ArmedSleep {
    fn new() -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(Duration::from_secs(100))),
            armed: false,
        }
    }

    fn reset(&mut self) {
        self.sleep
            .as_mut()
            .reset(Instant::now() + Duration::from_secs(100));
        self.armed = false;
    }

    fn arm(&mut self) {
        if self.armed {
            return;
        }

        self.sleep
            .as_mut()
            .reset(Instant::now() + Duration::from_millis(25));
        self.armed = true;
    }
}
