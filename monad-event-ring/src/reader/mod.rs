use std::marker::PhantomData;

pub(crate) use self::raw::RawEventReader;
pub use self::snapshot::SnapshotEventReader;
use crate::{EventDescriptor, EventNextResult, EventRingType};

mod raw;
mod snapshot;

/// A unified interface for iterating over events produced by a
/// [`TypedEventRing`](crate::TypedEventRing).
pub trait TypedEventReader<'ring> {
    /// The event produced by this reader.
    type Event<'reader>
    where
        Self: 'reader;

    /// Produces the next event in the ring.
    fn next<'reader>(&'reader mut self) -> Self::Event<'reader>;
}

/// An iterator over events in an [`EventRing`](crate::EventRing).
pub struct EventReader<'ring, T>
where
    T: EventRingType,
{
    pub(crate) raw: RawEventReader<'ring>,
    _phantom: PhantomData<T>,
}

impl<'ring, T> EventReader<'ring, T>
where
    T: EventRingType,
{
    pub(crate) fn new(raw: RawEventReader<'ring>) -> Self {
        Self {
            raw,
            _phantom: PhantomData,
        }
    }
}

impl<'ring, T> TypedEventReader<'ring> for EventReader<'ring, T>
where
    T: EventRingType,
{
    type Event<'reader>
        = EventNextResult<EventDescriptor<'ring, 'reader, T>>
    where
        Self: 'reader;

    fn next<'reader>(&'reader mut self) -> Self::Event<'reader> {
        self.raw.next().map(EventDescriptor::new)
    }
}
