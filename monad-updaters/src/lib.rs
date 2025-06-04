use std::{future::Future, pin::Pin};

use futures::Stream;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};

pub mod checkpoint;
pub mod ledger;
pub mod loopback;
pub mod parent;
pub mod state_root_hash;
pub mod statesync;
pub mod timestamp;
pub mod txpool;

#[cfg(feature = "tokio")]
pub mod config_loader;

#[cfg(all(feature = "tokio", feature = "monad-triedb"))]
pub mod triedb_state_root_hash;

#[cfg(feature = "tokio")]
pub mod timer;

#[cfg(feature = "tokio")]
pub mod tokio_timestamp;

#[cfg(feature = "tokio")]
pub mod local_router;

/// An Updater executes commands and produces events for State
pub trait Updater<E>: Executor + Stream<Item = E> {
    fn boxed<'a>(self) -> BoxUpdater<'a, Self::Command, E>
    where
        Self: Sized + Send + Unpin + 'a,
    {
        Box::pin(self)
    }
}
impl<U, E> Updater<E> for U where U: Executor + Stream<Item = E> {}

pub type BoxUpdater<'a, C, E> = Pin<Box<dyn Updater<E, Command = C> + Send + Unpin + 'a>>;

const DEFAULT_COMMAND_BUFFER_SIZE: usize = 1024;
const DEFAULT_EVENT_BUFFER_SIZE: usize = 1024;

#[cfg(feature = "tokio")]
pub struct TokioTaskUpdater<U, E>
where
    U: Updater<E>,
    U::Command: Send + 'static,
    E: Send + 'static,
{
    handle: tokio::task::JoinHandle<()>,
    metrics: ExecutorMetrics,
    update_metrics: Box<dyn Fn(&mut ExecutorMetrics)>,

    command_tx: tokio::sync::mpsc::Sender<Vec<U::Command>>,
    event_rx: tokio::sync::mpsc::Receiver<E>,
}

#[cfg(feature = "tokio")]
impl<U, E> TokioTaskUpdater<U, E>
where
    U: Updater<E>,
    U::Command: Send + 'static,
    E: Send + 'static,
{
    pub fn new<F>(
        updater: impl FnOnce(tokio::sync::mpsc::Receiver<Vec<U::Command>>, tokio::sync::mpsc::Sender<E>) -> F
            + Send
            + 'static,
        update_metrics: Box<dyn Fn(&mut ExecutorMetrics) + Send + 'static>,
    ) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self::new_with_buffer_sizes(
            updater,
            update_metrics,
            DEFAULT_COMMAND_BUFFER_SIZE,
            DEFAULT_EVENT_BUFFER_SIZE,
        )
    }

    pub fn new_with_buffer_sizes<F>(
        updater: impl FnOnce(tokio::sync::mpsc::Receiver<Vec<U::Command>>, tokio::sync::mpsc::Sender<E>) -> F
            + Send
            + 'static,
        update_metrics: Box<dyn Fn(&mut ExecutorMetrics) + Send + 'static>,
        command_buffer_size: usize,
        event_buffer_size: usize,
    ) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (command_tx, command_rx) = tokio::sync::mpsc::channel(command_buffer_size);
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(event_buffer_size);

        let handle = tokio::spawn(updater(command_rx, event_tx));

        Self {
            handle,
            metrics: ExecutorMetrics::default(),
            update_metrics,

            command_tx,
            event_rx,
        }
    }

    fn verify_handle_liveness(&self) {
        if self.handle.is_finished() {
            panic!("ThreadUpdater handle terminated!");
        }

        if self.command_tx.is_closed() {
            panic!("ThreadUpdater command_rx dropped!");
        }

        if self.event_rx.is_closed() {
            panic!("ThreadUpdater event_tx dropped!");
        }
    }
}

#[cfg(feature = "tokio")]
impl<U, E> Executor for TokioTaskUpdater<U, E>
where
    U: Updater<E>,
    U::Command: Send + 'static,
    E: Send + 'static,
{
    type Command = U::Command;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        self.verify_handle_liveness();

        self.command_tx
            .try_send(commands)
            .expect("executor is lagging")
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        ExecutorMetricsChain::from(&self.metrics)
    }
}

#[cfg(feature = "tokio")]
impl<U, E> Stream for TokioTaskUpdater<U, E>
where
    U: Updater<E>,
    U::Command: Send + 'static,
    E: Send + 'static,
{
    type Item = U::Item;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        this.verify_handle_liveness();

        (this.update_metrics)(&mut this.metrics);

        this.event_rx.poll_recv(cx)
    }
}
