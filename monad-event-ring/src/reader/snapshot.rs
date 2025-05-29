use super::{EventReader, TypedEventReader};
use crate::{EventNextResult, EventRingType, SnapshotEventDescriptor};

pub struct SnapshotEventReader<'ring, T>
where
    T: EventRingType,
{
    reader: EventReader<'ring, T>,
}

impl<'ring, T> SnapshotEventReader<'ring, T>
where
    T: EventRingType,
{
    pub(crate) fn new(mut reader: EventReader<'ring, T>) -> Self {
        reader.raw.inner.read_last_seqno = 0;

        Self { reader }
    }
}

impl<'ring, T> TypedEventReader<'ring> for SnapshotEventReader<'ring, T>
where
    T: EventRingType,
{
    type Event<'reader>
        = Option<SnapshotEventDescriptor<'ring, 'reader, T>>
    where
        Self: 'reader;

    fn next<'reader>(&'reader mut self) -> Self::Event<'reader> {
        match self.reader.next() {
            EventNextResult::Ready(event_descriptor) => {
                Some(SnapshotEventDescriptor::new(event_descriptor))
            }
            EventNextResult::NotReady => None,
            EventNextResult::Gap => panic!("SnapshotEventReader cannot gap!"),
        }
    }
}
