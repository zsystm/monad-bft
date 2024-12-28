use std::{
    marker::PhantomData,
    mem,
    ops::DerefMut,
    task::{Poll, Waker},
};

use bytes::Bytes;
use futures::Stream;
use monad_consensus_types::{block::ExecutionProtocol, signature_collection::SignatureCollection};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{MempoolEvent, MonadEvent};

pub struct MockIpcReceiver<ST, SCT, EPT> {
    transactions: Vec<Bytes>,

    waker: Option<Waker>,
    _phantom: PhantomData<(ST, SCT, EPT)>,
}

impl<ST, SCT, EPT> Default for MockIpcReceiver<ST, SCT, EPT> {
    fn default() -> Self {
        Self {
            transactions: Default::default(),
            waker: Default::default(),
            _phantom: PhantomData,
        }
    }
}

impl<ST, SCT, EPT> MockIpcReceiver<ST, SCT, EPT> {
    pub fn add_transaction(&mut self, txn: Bytes) {
        self.transactions.push(txn);

        if let Some(waker) = self.waker.take() {
            waker.wake()
        };
    }

    pub fn ready(&self) -> bool {
        !self.transactions.is_empty()
    }
}

impl<ST, SCT, EPT> Stream for MockIpcReceiver<ST, SCT, EPT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Item = MonadEvent<ST, SCT, EPT>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if this.transactions.is_empty() {
            this.waker = Some(cx.waker().clone());
            Poll::Pending
        } else {
            let txn_bytes = mem::take(&mut this.transactions);
            Poll::Ready(Some(MonadEvent::MempoolEvent(MempoolEvent::UserTxns(
                txn_bytes,
            ))))
        }
    }
}
