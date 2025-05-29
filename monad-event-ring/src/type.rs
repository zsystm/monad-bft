use crate::{
    ffi::{monad_event_ring_type, monad_event_ring_type_MONAD_EVENT_RING_TYPE_TEST},
    RawEventDescriptorInfo,
};

pub trait EventRingType: 'static {
    type Event;
    type EventRef<'reader>;

    fn ring_ctype() -> monad_event_ring_type;
    fn ring_metadata_hash() -> &'static [u8; 32];

    fn raw_to_event_ref<'reader>(
        info: RawEventDescriptorInfo,
        bytes: &'reader [u8],
    ) -> Self::EventRef<'reader>;

    fn event_ref_to_event<'reader>(event_ref: Self::EventRef<'reader>) -> Self::Event;
}

pub struct TestEventRingType;

impl EventRingType for TestEventRingType {
    type Event = (u16, Vec<u8>);
    type EventRef<'reader> = (u16, &'reader [u8]);

    fn ring_ctype() -> monad_event_ring_type {
        monad_event_ring_type_MONAD_EVENT_RING_TYPE_TEST
    }

    fn ring_metadata_hash() -> &'static [u8; 32] {
        todo!()
    }

    fn raw_to_event_ref<'reader>(
        info: RawEventDescriptorInfo,
        bytes: &'reader [u8],
    ) -> Self::EventRef<'reader> {
        (info.event_type, bytes)
    }

    fn event_ref_to_event<'reader>(
        (event_type, event_ref): Self::EventRef<'reader>,
    ) -> Self::Event {
        (event_type, event_ref.to_vec())
    }
}
