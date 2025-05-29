use std::marker::PhantomData;

pub(crate) use self::raw::RawEventReader;
pub use self::snapshot::SnapshotEventReader;
use crate::{EventDescriptor, EventNextResult, EventRingType};

mod raw;
mod snapshot;

pub trait TypedEventReader<'ring> {
    type Event<'reader>
    where
        Self: 'reader;

    fn next<'reader>(&'reader mut self) -> Self::Event<'reader>;
}

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
