use std::marker::PhantomData;

pub(crate) use self::raw::RawEventDescriptor;
use self::raw::RawEventDescriptorInfo;
use crate::{EventDecoder, EventDescriptorPayload};

mod raw;

/// The metadata for an event in an [`EventRing`](crate::EventRing).
#[derive(Debug)]
pub struct EventDescriptor<'ring, D>
where
    D: EventDecoder,
{
    raw: RawEventDescriptor<'ring>,
    _phantom: PhantomData<D>,
}

impl<'ring, D> EventDescriptor<'ring, D>
where
    D: EventDecoder,
{
    pub(crate) fn new(raw: RawEventDescriptor<'ring>) -> Self {
        Self {
            raw,
            _phantom: PhantomData,
        }
    }

    /// Attempts to read the payload associated with this event descriptor as the associated
    /// [`T::Event`](EventDecoder::Event) type.
    pub fn try_read(&self) -> EventDescriptorPayload<D::Event> {
        self.raw.try_filter_map(|raw_info, bytes| {
            let info = EventDescriptorInfo::new(raw_info);

            let event_ref = D::raw_to_event_ref(info, bytes);

            D::event_ref_to_event(event_ref)
        })
    }

    /// Attempts to selectively read the payload associated with this event descriptor to a
    /// user-specified type.
    ///
    /// This function enables a zero-copy API by allowing downstream consumers to `filter_map` the
    /// [`D::EventRef`](EventDecoder::EventRef) type which is a zero-copy view of the underlying
    /// bytes.
    ///
    /// <div class="warning">
    ///
    /// The `filter_map` function `f` **must** be a pure function and thus side-effect free. During
    /// `f`'s execution, it is possible for the underlying paylod bytes to be partially or
    /// completely overwritten which invalidates the zero-copy
    /// [`D::EventRef`](EventDecoder::EventRef). In this case, the result of the `filter_map` must
    /// be discarded, which is expressed through the [`EventDescriptorPayload::Expired`] variant.
    /// This requirement is further hinted at through the type definition for `f` which is
    /// intentionally a function pointer instead of a closure to avoid accidentally setting state
    /// outside the `filter_map`. Downstream consumers should **not** attempt to circumvent this
    /// behavior.
    ///
    /// </div>
    pub fn try_filter_map<R: 'static>(
        &self,
        f: fn(event_ref: D::EventRef<'_>) -> Option<R>,
    ) -> EventDescriptorPayload<Option<R>> {
        self.raw.try_filter_map(move |raw_info, bytes| {
            let info = EventDescriptorInfo::new(raw_info);

            let event_ref = D::raw_to_event_ref(info, bytes);

            f(event_ref)
        })
    }

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
    pub fn try_filter_map_raw<R: 'static>(
        &self,
        f: fn(info: EventDescriptorInfo<D>, payload_bytes: &[u8]) -> Option<R>,
    ) -> EventDescriptorPayload<Option<R>> {
        self.raw.try_filter_map(move |raw_info, bytes| {
            let info = EventDescriptorInfo::new(raw_info);

            f(info, bytes)
        })
    }
}

/// Information associated with an event descriptor.
pub struct EventDescriptorInfo<D>
where
    D: EventDecoder,
{
    /// Sequence number used to check liveness / detect gapping.
    pub seqno: u64,

    /// Enables distinguishing between variadic inner event types.
    ///
    /// See [`EventDecoder`] for more details.
    pub event_type: u16,

    /// The flow information associated with this event descriptor,
    ///
    /// See [`EventDecoder::FlowInfo`] for more details.
    pub flow_info: D::FlowInfo,
}

impl<D> EventDescriptorInfo<D>
where
    D: EventDecoder,
{
    fn new(raw: RawEventDescriptorInfo) -> Self {
        Self {
            seqno: raw.seqno,
            event_type: raw.event_type,
            flow_info: D::transmute_flow_info(raw.user),
        }
    }
}
