use monad_event_ring::{EventDescriptor, EventReader};

use crate::{
    ffi::{
        monad_c_bytes32, monad_exec_iter_block_id_prev, monad_exec_iter_block_number_prev,
        monad_exec_iter_consensus_prev, monad_exec_ring_block_id_matches,
        monad_exec_ring_get_block_number,
    },
    ExecEventDecoder, ExecEventType,
};

/// Provides utilities for [`ExecEventDecoder`] [`EventDescriptor`]s.
pub trait ExecEventDescriptorExt {
    /// Produces the block number associated with the current [`EventDescriptor`].
    fn get_block_number(&self) -> Result<u64, ()>;

    /// Checks whether the current [`EventDescriptor`] is associated with the provided `block_id`.
    fn block_id_matches(&self, block_id: &monad_c_bytes32) -> bool;
}

impl<'ring> ExecEventDescriptorExt for EventDescriptor<'ring, ExecEventDecoder> {
    fn get_block_number(&self) -> Result<u64, ()> {
        self.with_raw(monad_exec_ring_get_block_number)
    }

    fn block_id_matches(&self, block_id: &monad_c_bytes32) -> bool {
        self.with_raw(|c_event_ring, c_event_descriptor| {
            monad_exec_ring_block_id_matches(c_event_ring, c_event_descriptor, block_id)
        })
    }
}

/// Provides utilities for [`ExecEventDecoder`] [`EventReader`]s.
pub trait ExecEventReaderExt<'ring> {
    /// Rewinds the [`EventReader`] to the last consensus event specified by the `filter` argument,
    /// producing an [`EventDescriptor`] to it.
    ///
    /// If this method succeeds, the next call to [`EventReader::next_descriptor`] produce the same
    /// event descriptor. Otherwise, the reader remains unchanged.
    fn consensus_prev(
        &mut self,
        filter: ExecEventType,
    ) -> Result<EventDescriptor<'ring, ExecEventDecoder>, ()>;

    /// Rewinds the [`EventReader`] to the last event in the provided `block_number` specified by
    /// the `filter` argument.
    ///
    /// If this method succeeds, the next call to [`EventReader::next_descriptor`] produce the same
    /// event descriptor. Otherwise, the reader remains unchanged.
    fn block_number_prev(
        &mut self,
        block_number: u64,
        filter: ExecEventType,
    ) -> Result<EventDescriptor<'ring, ExecEventDecoder>, ()>;

    /// Rewinds the [`EventReader`] to the last event in the provided `block_id` specified by the
    /// `filter` argument.
    ///
    /// If this method succeeds, the next call to [`EventReader::next_descriptor`] produce the same
    /// event descriptor. Otherwise, the reader remains unchanged.
    fn block_id_prev(
        &mut self,
        block_id: &monad_c_bytes32,
        filter: ExecEventType,
    ) -> Result<EventDescriptor<'ring, ExecEventDecoder>, ()>;
}

impl<'ring> ExecEventReaderExt<'ring> for EventReader<'ring, ExecEventDecoder> {
    fn consensus_prev(
        &mut self,
        filter: ExecEventType,
    ) -> Result<EventDescriptor<'ring, ExecEventDecoder>, ()> {
        self.with_raw(|c_event_ring, c_event_iterator| {
            monad_exec_iter_consensus_prev(c_event_iterator, c_event_ring, filter.as_event_type())
        })
    }

    fn block_number_prev(
        &mut self,
        block_number: u64,
        filter: ExecEventType,
    ) -> Result<EventDescriptor<'ring, ExecEventDecoder>, ()> {
        self.with_raw(|c_event_ring, c_event_iterator| {
            monad_exec_iter_block_number_prev(
                c_event_iterator,
                c_event_ring,
                block_number,
                filter.as_event_type(),
            )
        })
    }

    fn block_id_prev(
        &mut self,
        block_id: &monad_c_bytes32,
        filter: ExecEventType,
    ) -> Result<EventDescriptor<'ring, ExecEventDecoder>, ()> {
        self.with_raw(|c_event_ring, c_event_iterator| {
            monad_exec_iter_block_id_prev(
                c_event_iterator,
                c_event_ring,
                block_id,
                filter.as_event_type(),
            )
        })
    }
}
