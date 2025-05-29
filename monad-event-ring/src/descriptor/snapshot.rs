use super::{EventDescriptor, RawEventDescriptorInfo, TypedEventDescriptor};
use crate::{EventDescriptorPayload, EventRingType};

/// The metadata for an event in a [`SnapshotEventRing`](crate::SnapshotEventRing).
///
/// Unlike the [`EventDescriptor`] variant, this event descriptor produces events directly and does
/// produce [`EventDescriptorPayload::Expired`]-like variants since snapshot rings are backed by
/// static files and thus by definition the payload cannot become expired.
#[derive(Debug)]
pub struct SnapshotEventDescriptor<'ring, 'reader, T>
where
    T: EventRingType,
{
    descriptor: EventDescriptor<'ring, 'reader, T>,
}

impl<'ring, 'reader, T> SnapshotEventDescriptor<'ring, 'reader, T>
where
    T: EventRingType,
{
    pub(crate) fn new(descriptor: EventDescriptor<'ring, 'reader, T>) -> Self {
        Self { descriptor }
    }
}

impl<'ring, 'reader, T> TypedEventDescriptor<'ring, 'reader, T>
    for SnapshotEventDescriptor<'ring, 'reader, T>
where
    T: EventRingType,
{
    type PayloadResult<E>
        = E
    where
        E: 'ring + 'reader;

    fn try_read(&self) -> Self::PayloadResult<T::Event> {
        unwrap_snapshot_payload(self.descriptor.try_read())
    }

    fn try_filter_map<R: 'static>(
        &self,
        f: fn(event_ref: T::EventRef<'_>) -> Option<R>,
    ) -> Self::PayloadResult<Option<R>> {
        unwrap_snapshot_payload(self.descriptor.try_filter_map(f))
    }

    fn try_filter_map_raw<R: 'static>(
        &self,
        f: fn(info: RawEventDescriptorInfo, payload_bytes: &[u8]) -> Option<R>,
    ) -> Self::PayloadResult<Option<R>> {
        unwrap_snapshot_payload(self.descriptor.try_filter_map_raw(f))
    }
}

fn unwrap_snapshot_payload<T>(value: EventDescriptorPayload<T>) -> T {
    match value {
        EventDescriptorPayload::Payload(value) => value,
        EventDescriptorPayload::Expired => {
            panic!("SnapshotEventDescriptor produced expired payload!")
        }
    }
}
