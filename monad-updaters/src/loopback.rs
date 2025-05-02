use std::{
    collections::VecDeque,
    marker::PhantomData,
    ops::DerefMut,
    task::{Poll, Waker},
};

use futures::Stream;
use monad_executor::Executor;
use monad_executor_glue::LoopbackCommand;
use monad_metrics::MetricsPolicy;

/// The loopback executor routes outputs from one child state to another. The
/// update happens asynchronously and the event/operation must be idempotent on
/// the target state as replay can cause duplicate event to be applied.
///
/// e.g. StateRootUpdate for ConsensusState is idempotent because inserting the
/// same value to a map multiple times doesn't change the final state
pub struct LoopbackExecutor<E, MP>
where
    MP: MetricsPolicy,
{
    /// Buffered events to send back
    buffer: VecDeque<E>,
    waker: Option<Waker>,
    _phantom: PhantomData<MP>,
}

impl<E, MP> Default for LoopbackExecutor<E, MP>
where
    MP: MetricsPolicy,
{
    fn default() -> Self {
        Self {
            buffer: Default::default(),
            waker: Default::default(),
            _phantom: PhantomData,
        }
    }
}

impl<E, MP> Executor<MP> for LoopbackExecutor<E, MP>
where
    MP: MetricsPolicy,
{
    type Command = LoopbackCommand<E>;
    type Metrics = ();

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for cmd in commands {
            match cmd {
                LoopbackCommand::Forward(event) => self.buffer.push_back(event),
            }
        }

        if self.ready() {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            }
        }
    }

    fn metrics(&self) -> &Self::Metrics {
        &()
    }
}

impl<E, MP> Stream for LoopbackExecutor<E, MP>
where
    MP: MetricsPolicy,
    Self: Unpin,
{
    type Item = E;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some(e) = this.buffer.pop_front() {
            Poll::Ready(Some(e))
        } else {
            this.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl<E, MP> LoopbackExecutor<E, MP>
where
    MP: MetricsPolicy,
{
    pub fn ready(&self) -> bool {
        !self.buffer.is_empty()
    }
}
