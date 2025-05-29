use std::marker::PhantomData;

pub use self::{
    raw::{RawEventDescriptor, RawEventDescriptorInfo},
    snapshot::SnapshotEventDescriptor,
};
use crate::EventRingType;

mod raw;
mod snapshot;

#[derive(Debug)]
pub struct EventDescriptor<'ring, 'reader, T>
where
    T: EventRingType,
{
    raw: RawEventDescriptor<'ring, 'reader>,
    _phantom: PhantomData<T>,
}

impl<'ring, 'reader, T> EventDescriptor<'ring, 'reader, T>
where
    T: EventRingType,
{
    pub(crate) fn new(raw: RawEventDescriptor<'ring, 'reader>) -> Self {
        Self {
            raw,
            _phantom: PhantomData,
        }
    }

    pub fn try_read_payload(&self) -> EventDescriptorPayload<T::Event> {
        self.raw.try_map_payload(|event_type, bytes| {
            let event_ref = T::raw_to_event_ref(event_type, bytes);

            T::event_ref_to_event(event_ref)
        })
    }

    pub fn try_map_payload<R>(
        &self,
        f: fn(T::EventRef<'_>) -> Option<R>,
    ) -> EventDescriptorPayload<Option<R>> {
        self.raw.try_map_payload(move |event_type, bytes| {
            let event_ref = T::raw_to_event_ref(event_type, bytes);

            f(event_ref)
        })
    }
}

pub enum EventDescriptorPayload<T> {
    Payload(T),
    Expired,
}

impl<T> EventDescriptorPayload<T> {
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> EventDescriptorPayload<U> {
        match self {
            EventDescriptorPayload::Payload(payload) => EventDescriptorPayload::Payload(f(payload)),
            EventDescriptorPayload::Expired => EventDescriptorPayload::Expired,
        }
    }
}
