use std::marker::PhantomData;

pub(crate) use self::raw::RawEventDescriptor;
pub use self::{raw::RawEventDescriptorInfo, snapshot::SnapshotEventDescriptor};
use crate::{EventDescriptorPayload, EventRingType};

mod raw;
mod snapshot;

/// A unified interface for reading paylods from events produced by a
/// [`TypedEventReader`](crate::TypedEventReader).
pub trait TypedEventDescriptor<'ring, 'reader, T>
where
    T: EventRingType + 'ring + 'reader,
{
    /// Describes how a payload is produced from this event descriptor.
    type PayloadResult<E>
    where
        E: 'ring + 'reader;

    /// Attempts to read the payload associated with this event descriptor as the associated
    /// [`T::Event`](EventRingType::Event) type.
    fn try_read(&self) -> Self::PayloadResult<T::Event>;

    /// Attempts to selectively read the payload associated with this event descriptor to a
    /// user-specified type.
    ///
    /// This function enables a zero-copy API by allowing downstream consumers to `filter_map` the
    /// [`T::EventRef`](EventRingType::EventRef) type which is a zero-copy view of the underlying
    /// bytes.
    ///
    /// <div class="warning">
    ///
    /// The `filter_map` function `f` **must** be a pure function and thus side-effect free. During
    /// `f`'s execution, it is possible for the underlying paylod bytes to be partially or
    /// completely overwritten which invalidates the zero-copy
    /// [`T::EventRef`](EventRingType::EventRef). In this case, the result of the `filter_map` must
    /// be discarded, which is expressed through the [`EventDescriptorPayload::Expired`] variant.
    /// This requirement is further hinted at through the type definition for `f` which is
    /// intentionally a function pointer instead of a closure to avoid accidentally setting state
    /// outside the `filter_map`. Downstream consumers should **not** attempt to circumvent this
    /// behavior.
    ///
    /// </div>
    fn try_filter_map<R: 'static>(
        &self,
        f: fn(event_ref: T::EventRef<'_>) -> Option<R>,
    ) -> Self::PayloadResult<Option<R>>;

    /// Attempts to selectively read the payload byte slice associated with this event descriptor to
    /// a user-specified type.
    ///
    /// This function enables a zero-copy API by providing downstream consumers the underlying
    /// payload byte slice. This method should **not** be used unless you explicitly need to work
    /// at a byte-level view.
    ///
    /// <div class="warning">
    ///
    /// See [`try_filter_map`](EventDescriptor::try_filter_map) for important semantics about `f`.
    ///
    /// </div>
    fn try_filter_map_raw<R: 'static>(
        &self,
        f: fn(info: RawEventDescriptorInfo, payload_bytes: &[u8]) -> Option<R>,
    ) -> Self::PayloadResult<Option<R>>;
}

/// The metadata for an event in an [`EventRing`](crate::EventRing).
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
}

impl<'ring, 'reader, T> TypedEventDescriptor<'ring, 'reader, T>
    for EventDescriptor<'ring, 'reader, T>
where
    T: EventRingType,
{
    type PayloadResult<E>
        = EventDescriptorPayload<E>
    where
        E: 'ring + 'reader;

    fn try_read(&self) -> Self::PayloadResult<T::Event> {
        self.raw.try_filter_map(|event_type, bytes| {
            let event_ref = T::raw_to_event_ref(event_type, bytes);

            T::event_ref_to_event(event_ref)
        })
    }

    fn try_filter_map<R: 'static>(
        &self,
        f: fn(event_ref: T::EventRef<'_>) -> Option<R>,
    ) -> Self::PayloadResult<Option<R>> {
        self.raw.try_filter_map(move |event_type, bytes| {
            let event_ref = T::raw_to_event_ref(event_type, bytes);

            f(event_ref)
        })
    }

    fn try_filter_map_raw<R: 'static>(
        &self,
        f: fn(info: RawEventDescriptorInfo, payload_bytes: &[u8]) -> Option<R>,
    ) -> Self::PayloadResult<Option<R>> {
        self.raw
            .try_filter_map(move |event_type, bytes| f(event_type, bytes))
    }
}
