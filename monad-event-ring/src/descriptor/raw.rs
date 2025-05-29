use crate::{
    ffi::{monad_event_descriptor, monad_event_payload_check, monad_event_payload_peek},
    EventDescriptorPayload, RawEventReader,
};

#[derive(Debug)]
pub(crate) struct RawEventDescriptor<'ring, 'reader> {
    inner: monad_event_descriptor,
    reader: &'reader mut RawEventReader<'ring>,
}

impl<'ring, 'reader> RawEventDescriptor<'ring, 'reader> {
    pub(crate) fn new(
        reader: &'reader mut RawEventReader<'ring>,
        c_event_descriptor: monad_event_descriptor,
    ) -> Self {
        Self {
            inner: c_event_descriptor,
            reader,
        }
    }

    pub fn try_filter_map<T>(
        &self,
        f: impl FnOnce(RawEventDescriptorInfo, &[u8]) -> T,
    ) -> EventDescriptorPayload<T> {
        let Some(bytes) = monad_event_payload_peek(&self.reader.inner, &self.inner) else {
            return EventDescriptorPayload::Expired;
        };

        let value = f(
            RawEventDescriptorInfo {
                seqno: self.inner.seqno,
                event_type: self.inner.event_type,
                user: self.inner.user,
            },
            bytes,
        );

        if monad_event_payload_check(&self.reader.inner, &self.inner) {
            EventDescriptorPayload::Payload(value)
        } else {
            EventDescriptorPayload::Expired
        }
    }
}

/// Information associated to an event descriptor.
pub struct RawEventDescriptorInfo {
    /// Sequence number used to check liveness / detect gapping.
    pub seqno: u64,

    /// Enables distinguishing between variadic inner event types.
    ///
    /// See [`EventRingType`](crate::EventRingType) for more details.
    pub event_type: u16,

    /// A buffer used to store additional information.
    ///
    /// See [`EventRingType`](crate::EventRingType) for more details.
    pub user: [u64; 4],
}
