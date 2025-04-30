use std::{future::Future, marker::PhantomData, pin::Pin};

use futures::Stream;
use monad_executor::Executor;
use monad_metrics::MetricsPolicy;
use pin_project::pin_project;

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
pub trait Updater<E, MP>: Executor<MP> + Stream<Item = E>
where
    MP: MetricsPolicy,
{
    fn boxed<'a>(self) -> BoxUpdater<'a, E, MP, Self::Command, Self::Metrics>
    where
        Self: Sized + Send + Unpin + 'a,
    {
        Box::pin(self)
    }
}

impl<U, E, MP> Updater<E, MP> for U
where
    U: Executor<MP> + Stream<Item = E> + Send + Unpin,
    MP: MetricsPolicy,
{
}

pub type BoxUpdater<'a, E, MP, C, M> =
    Pin<Box<dyn Updater<E, MP, Command = C, Metrics = M> + Send + Unpin + 'a>>;

#[pin_project]
pub struct VoidMetricUpdater<U, E, MP>
where
    U: Updater<E, MP>,
    MP: MetricsPolicy,
{
    #[pin]
    updater: U,
    _phantom: PhantomData<(E, MP)>,
}

impl<U, E, MP> VoidMetricUpdater<U, E, MP>
where
    U: Updater<E, MP>,
    MP: MetricsPolicy,
{
    pub fn new(updater: U) -> Self {
        Self {
            updater,
            _phantom: PhantomData,
        }
    }
}

impl<U, E, MP> Stream for VoidMetricUpdater<U, E, MP>
where
    U: Updater<E, MP>,
    MP: MetricsPolicy,
{
    type Item = E;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().updater.poll_next(cx)
    }
}

impl<U, E, MP> Executor<MP> for VoidMetricUpdater<U, E, MP>
where
    U: Updater<E, MP>,
    MP: MetricsPolicy,
{
    type Command = U::Command;
    type Metrics = ();

    fn exec(&mut self, commands: Vec<Self::Command>) {
        self.updater.exec(commands);
    }

    fn metrics(&self) -> &Self::Metrics {
        &()
    }
}

const DEFAULT_COMMAND_BUFFER_SIZE: usize = 1024;
const DEFAULT_EVENT_BUFFER_SIZE: usize = 1024;

#[cfg(feature = "tokio")]
pub struct TokioTaskUpdater<U, E, MP>
where
    U: Updater<E, MP>,
    U::Command: Send + 'static,
    E: Send + 'static,
    MP: MetricsPolicy,
{
    handle: tokio::task::JoinHandle<()>,
    metrics: U::Metrics,
    command_tx: tokio::sync::mpsc::Sender<Vec<U::Command>>,
    event_rx: tokio::sync::mpsc::Receiver<E>,
}

#[cfg(feature = "tokio")]
impl<U, E, MP> TokioTaskUpdater<U, E, MP>
where
    U: Updater<E, MP>,
    U::Command: Send + 'static,
    E: Send + 'static,
    MP: MetricsPolicy,
{
    pub fn new<F>(
        metrics: U::Metrics,
        updater: impl FnOnce(tokio::sync::mpsc::Receiver<Vec<U::Command>>, tokio::sync::mpsc::Sender<E>) -> F
            + Send
            + 'static,
    ) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self::new_with_buffer_sizes(
            metrics,
            updater,
            DEFAULT_COMMAND_BUFFER_SIZE,
            DEFAULT_EVENT_BUFFER_SIZE,
        )
    }

    pub fn new_with_buffer_sizes<F>(
        metrics: U::Metrics,
        updater: impl FnOnce(tokio::sync::mpsc::Receiver<Vec<U::Command>>, tokio::sync::mpsc::Sender<E>) -> F
            + Send
            + 'static,
        command_buffer_size: usize,
        event_buffer_size: usize,
    ) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (command_tx, command_rx) = tokio::sync::mpsc::channel(1024);
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(1024);

        let handle = tokio::spawn(updater(command_rx, event_tx));

        Self {
            handle,
            metrics,
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
impl<U, E, MP> Executor<MP> for TokioTaskUpdater<U, E, MP>
where
    U: Updater<E, MP>,
    U::Command: Send + 'static,
    E: Send + 'static,
    MP: MetricsPolicy,
{
    type Command = U::Command;
    type Metrics = U::Metrics;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        self.verify_handle_liveness();

        self.command_tx
            .try_send(commands)
            .expect("executor is lagging")
    }

    fn metrics(&self) -> &Self::Metrics {
        &self.metrics
    }
}

#[cfg(feature = "tokio")]
impl<U, E, MP> Stream for TokioTaskUpdater<U, E, MP>
where
    U: Updater<E, MP>,
    U::Command: Send + 'static,
    E: Send + 'static,
    MP: MetricsPolicy,
    Self: Unpin,
{
    type Item = U::Item;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.verify_handle_liveness();

        self.event_rx.poll_recv(cx)
    }
}
