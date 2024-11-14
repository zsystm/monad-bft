use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

use futures::Stream;
use monad_consensus_types::signature_collection::SignatureCollection;
<<<<<<< HEAD
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, TimestampCommand};
use monad_types::ExecutionProtocol;
use tokio::time::{Duration, Interval};

use crate::timestamp::TimestampAdjuster;

pub struct TokioTimestamp<ST, SCT, EPT> {
    /// create timestamp events at this interval
    interval: Interval,
    metrics: ExecutorMetrics,
    _phantom: PhantomData<(ST, SCT, EPT)>,
}

impl<ST, SCT, EPT> TokioTimestamp<ST, SCT, EPT> {
    pub fn new(period: Duration) -> Self {
        Self {
            interval: tokio::time::interval(period),
            metrics: Default::default(),

        Self {
            interval: tokio::time::interval(period),
            _phantom: PhantomData,
        }
    }
}

impl<ST, SCT, EPT> Executor for TokioTimestamp<ST, SCT, EPT> {
    type Command = TimestampCommand;

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT, EPT> Stream for TokioTimestamp<ST, SCT, EPT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Item = MonadEvent<ST, SCT, EPT>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        match this.interval.poll_tick(cx) {
            Poll::Ready(_) => Poll::Ready(Some(MonadEvent::TimestampUpdateEvent(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    .try_into()
                    .unwrap(),
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}
