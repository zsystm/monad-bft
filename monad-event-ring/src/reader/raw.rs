use super::TypedEventReader;
use crate::{
    ffi::{monad_event_iterator, monad_event_ring_iterator_init},
    ring::RawEventRing,
    EventNextResult, RawEventDescriptor,
};

#[derive(Debug)]
pub(crate) struct RawEventReader<'ring> {
    pub(crate) inner: monad_event_iterator,

    #[allow(unused)]
    event_ring: &'ring RawEventRing,
}

impl<'ring> RawEventReader<'ring> {
    pub(crate) fn new(event_ring: &'ring RawEventRing) -> Result<Self, String> {
        let inner = monad_event_ring_iterator_init(&event_ring.inner)?;

        Ok(Self { inner, event_ring })
    }
}

impl<'ring> TypedEventReader<'ring> for RawEventReader<'ring> {
    type Event<'reader>
        = EventNextResult<RawEventDescriptor<'ring, 'reader>>
    where
        Self: 'reader;

    fn next<'reader>(&'reader mut self) -> Self::Event<'reader> {
        EventNextResult::new_from_raw(self)
    }
}

unsafe impl<'ring> Send for RawEventReader<'ring> {}
unsafe impl<'ring> Sync for RawEventReader<'ring> {}
