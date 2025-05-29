use crate::{
    ffi::{self, monad_event_descriptor, monad_event_iterator_try_next, monad_event_next_result},
    RawEventDescriptor, RawEventReader,
};

pub enum EventNextResult<T> {
    Ready(T),
    NotReady,
    Gap,
}

impl<'ring, 'reader> EventNextResult<RawEventDescriptor<'ring, 'reader>> {
    pub(crate) fn new_from_raw(reader: &'reader mut RawEventReader<'ring>) -> Self {
        let (c_next_result, c_event_descriptor): (monad_event_next_result, monad_event_descriptor) =
            monad_event_iterator_try_next(&mut reader.inner);

        match c_next_result {
            ffi::monad_event_next_result_MONAD_EVENT_SUCCESS => {
                Self::Ready(RawEventDescriptor::new(reader, c_event_descriptor))
            }
            ffi::monad_event_next_result_MONAD_EVENT_NOT_READY => Self::NotReady,
            ffi::monad_event_next_result_MONAD_EVENT_GAP => Self::Gap,
            _ => panic!("EventNextResult encountered unknown value {c_next_result}"),
        }
    }

    pub(crate) fn map<T>(
        self,
        f: impl FnOnce(RawEventDescriptor<'ring, 'reader>) -> T,
    ) -> EventNextResult<T> {
        match self {
            EventNextResult::Ready(descriptor) => EventNextResult::Ready(f(descriptor)),
            EventNextResult::NotReady => EventNextResult::NotReady,
            EventNextResult::Gap => EventNextResult::Gap,
        }
    }
}
