use super::{EventDescriptor, EventDescriptorPayload};
use crate::EventRingType;

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

    pub fn try_read_payload(&self) -> T::Event {
        unwrap_snapshot_payload(self.descriptor.try_read_payload())
    }

    pub fn try_map_payload<R>(&self, f: fn(T::EventRef<'_>) -> Option<R>) -> Option<R> {
        unwrap_snapshot_payload(self.descriptor.try_map_payload(f))
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
