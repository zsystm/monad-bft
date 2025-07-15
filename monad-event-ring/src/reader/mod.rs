use std::marker::PhantomData;

pub(crate) use self::raw::RawEventReader;
use crate::{
    ffi::{monad_event_descriptor, monad_event_iterator, monad_event_ring},
    EventDecoder, EventDescriptor, EventNextResult, RawEventDescriptor,
};

mod raw;

/// Used to consume events from an [`EventRing`](crate::EventRing).
pub struct EventReader<'ring, D>
where
    D: EventDecoder,
{
    pub(crate) raw: RawEventReader<'ring>,
    _phantom: PhantomData<D>,
}

impl<'ring, D> EventReader<'ring, D>
where
    D: EventDecoder,
{
    pub(crate) fn new(raw: RawEventReader<'ring>) -> Self {
        Self {
            raw,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn new_snapshot(mut raw: RawEventReader<'ring>) -> Self {
        raw.inner.read_last_seqno = 0;

        Self {
            raw,
            _phantom: PhantomData,
        }
    }

    /// Produces the next event in the ring.
    pub fn next_descriptor(&mut self) -> EventNextResult<EventDescriptor<'ring, D>> {
        self.raw.next_descriptor().map(EventDescriptor::new)
    }

    /// Exposes the underlying c-types.
    pub fn with_raw(
        &mut self,
        f: impl FnOnce(
            &monad_event_ring,
            &mut monad_event_iterator,
        ) -> Result<monad_event_descriptor, ()>,
    ) -> Result<EventDescriptor<'ring, D>, ()> {
        let c_event_descriptor = f(&self.raw.event_ring.inner, &mut self.raw.inner)?;

        let raw_event_descriptor =
            RawEventDescriptor::new(&self.raw.event_ring, c_event_descriptor);

        Ok(EventDescriptor::new(raw_event_descriptor))
    }
}
