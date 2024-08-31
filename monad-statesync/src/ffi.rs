use std::{
    collections::HashMap,
    ffi::CString,
    ops::DerefMut,
    pin::{pin, Pin},
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{FutureExt, Stream};
use monad_crypto::certificate_signature::PubKey;
use monad_executor_glue::{StateSyncRequest, StateSyncResponse, StateSyncUpsertType};
use monad_types::{NodeId, SeqNum};
use rand::seq::SliceRandom;

use crate::{bindings, outbound_requests::OutboundRequests};

type StateSyncContext = Box<dyn FnMut(bindings::monad_sync_request)>;

// void (*statesync_send_request)(struct StateSync *, struct SyncRequest)
#[no_mangle]
pub extern "C" fn statesync_send_request(
    statesync: *mut bindings::monad_statesync_client,
    request: bindings::monad_sync_request,
) {
    let statesync = statesync as *mut StateSyncContext;
    unsafe { (*statesync)(request) }
}

pub(crate) struct StateSync<PT: PubKey> {
    state_sync_peers: Vec<NodeId<PT>>,
    target: SeqNum,
    outbound_requests: OutboundRequests,

    /// for each prefix, the node (if any) that all further responses must come from
    prefix_peers: HashMap<u64, NodeId<PT>>,

    execution_ctx: *mut bindings::monad_statesync_client_context,
    request_rx: tokio::sync::mpsc::UnboundedReceiver<bindings::monad_sync_request>,

    /// callback function that must be kept alive until statesync_execution_context_destroy is
    /// called
    _request_ctx: Box<Box<dyn FnMut(bindings::monad_sync_request)>>,

    progress: Option<Progress>,

    waker: Option<Waker>,
}

struct Progress {
    current_progress: u64,
    total_progress: u64,
    num_prefixes: u64,
}

pub(crate) struct Target {
    pub n: SeqNum,
    pub state_root: [u8; 32],
}

impl<PT: PubKey> StateSync<PT> {
    pub fn start(
        db_paths: &[String],
        genesis_path: &str,
        state_sync_peers: &[NodeId<PT>],
        max_parallel_requests: usize,
        request_timeout: Duration,
        target: Target,
    ) -> Self {
        let db_paths: Vec<CString> = db_paths
            .iter()
            .map(|path| {
                CString::new(path.to_owned()).expect("invalid db_path - does it contain null byte?")
            })
            .collect();
        let db_paths_ptrs: Vec<*const i8> = db_paths.iter().map(|s| s.as_ptr()).collect();
        let db_paths_ptr = db_paths_ptrs.as_ptr();
        let num_db_paths = db_paths_ptrs.len();
        let genesis_path =
            CString::new(genesis_path).expect("invalid genesis_path - does it contain null byte?");

        let (request_tx, request_rx) = tokio::sync::mpsc::unbounded_channel();

        let mut request_ctx: Box<StateSyncContext> = Box::new(Box::new(move |request| {
            let result = request_tx.send(request);
            if result.is_err() {
                eprintln!("invariant broken: send_request called after destroy");
                // we can't panic because that's not safe in a C callback
                std::process::exit(1)
            }
        }));
        let execution_ctx = unsafe {
            let ctx = bindings::monad_statesync_client_context_create(
                db_paths_ptr,
                num_db_paths,
                genesis_path.as_ptr(),
                &mut *request_ctx as *mut _ as *mut bindings::monad_statesync_client,
                Some(statesync_send_request),
            );

            bindings::monad_statesync_client_handle_target(
                ctx,
                bindings::monad_sync_target {
                    n: target.n.0,
                    state_root: target.state_root,
                },
            );

            ctx
        };

        Self {
            state_sync_peers: state_sync_peers.to_vec(),
            target: target.n,
            outbound_requests: OutboundRequests::new(max_parallel_requests, request_timeout),

            prefix_peers: Default::default(),

            execution_ctx,
            request_rx,

            _request_ctx: request_ctx,

            progress: None,

            waker: None,
        }
    }

    pub fn target(&self) -> SeqNum {
        self.target
    }

    pub fn handle_response(&mut self, from: NodeId<PT>, response: StateSyncResponse) {
        if !self.state_sync_peers.iter().any(|trusted| trusted == &from) {
            tracing::warn!(?from, "dropping state sync response from untrusted peer",);
            return;
        }
        let maybe_prefix_peer = self.prefix_peers.get(&response.request.prefix);
        if maybe_prefix_peer.is_some_and(|prefix_peer| prefix_peer != &from) {
            tracing::debug!(
                ?from,
                "dropping state sync response, already fixed to different prefix_peer"
            );
            return;
        }
        if self.outbound_requests.handle_response(&response) {
            // valid request
            unsafe {
                for (upsert_type, upsert_data) in response.response {
                    bindings::monad_statesync_client_handle_upsert(
                        self.execution_ctx,
                        match upsert_type {
                            StateSyncUpsertType::Code => {
                                bindings::monad_sync_type_SyncTypeUpsertCode
                            }
                            StateSyncUpsertType::Account => {
                                bindings::monad_sync_type_SyncTypeUpsertAccount
                            }
                            StateSyncUpsertType::Storage => {
                                bindings::monad_sync_type_SyncTypeUpsertStorage
                            }
                            StateSyncUpsertType::AccountDelete => {
                                bindings::monad_sync_type_SyncTypeUpsertAccountDelete
                            }
                            StateSyncUpsertType::StorageDelete => {
                                bindings::monad_sync_type_SyncTypeUpsertStorageDelete
                            }
                        },
                        upsert_data.as_ptr(),
                        upsert_data.len() as u64,
                    )
                }
                bindings::monad_statesync_client_handle_done(
                    self.execution_ctx,
                    bindings::monad_sync_done {
                        success: true,
                        prefix: response.request.prefix,
                        n: response.response_n,
                    },
                )
            }
            self.prefix_peers.insert(response.request.prefix, from);
            self.update_progress(&response.request);

            if let Some(waker) = self.waker.take() {
                waker.wake()
            }
        }
    }

    fn update_progress(&mut self, done_request: &StateSyncRequest) {
        let num_prefixes = 2_usize.pow(8).pow(done_request.prefix_bytes.into()) as u64;
        let total_progress = (done_request.target - done_request.old_target) * num_prefixes;
        let response_progress = done_request.until - done_request.from;
        let old_progress = self
            .progress
            .as_ref()
            .map_or(done_request.old_target * num_prefixes, |progress| {
                progress.current_progress
            });
        self.progress = Some(Progress {
            current_progress: old_progress + response_progress,
            total_progress,
            num_prefixes,
        });
    }

    /// An estimate of current sync progress in `Target` units
    pub fn progress_estimate(&self) -> Option<SeqNum> {
        // current_progress / num_prefixes can be used as a progress target estimate
        let progress = self.progress.as_ref()?;
        if progress.num_prefixes == 0 {
            return None;
        }
        Some(SeqNum(progress.current_progress / progress.num_prefixes))
    }

    fn has_reached_target(&self) -> bool {
        unsafe { bindings::monad_statesync_client_has_reached_target(self.execution_ctx) }
    }
}

impl<PT: PubKey> Drop for StateSync<PT> {
    fn drop(&mut self) {
        unsafe { bindings::monad_statesync_client_context_destroy(self.execution_ctx) }
    }
}

impl<PT: PubKey> Stream for StateSync<PT> {
    type Item = (NodeId<PT>, StateSyncRequest);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if this.has_reached_target() {
            let root_matches =
                unsafe { bindings::monad_statesync_client_finalize(this.execution_ctx) };
            assert!(root_matches, "state root doesn't match, are peers trusted?");
            assert!(this.outbound_requests.is_empty());

            // done statesyncing
            return Poll::Ready(None);
        }

        while let Poll::Ready(request) = this.request_rx.poll_recv(cx) {
            let request = request.expect("request_tx is never dropped");
            this.outbound_requests.queue_request(StateSyncRequest {
                prefix: request.prefix,
                prefix_bytes: request.prefix_bytes,
                target: request.target,
                from: request.from,
                until: request.until,
                old_target: request.old_target,
            });
        }

        let fut = this.outbound_requests.poll();
        if let Poll::Ready(request) = pin!(fut).poll_unpin(cx) {
            let servicer = this.prefix_peers.get(&request.prefix).unwrap_or_else(|| {
                this.state_sync_peers
                    .choose(&mut rand::thread_rng())
                    .expect("unable to send state-sync request, no peers")
            });
            return Poll::Ready(Some((*servicer, request)));
        }

        Poll::Pending
    }
}
