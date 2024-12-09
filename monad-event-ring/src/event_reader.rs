//! Module defining the EventReader object and its API

use std::{
    hint,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{event_ring::*, event_ring_util::check_event_ring_type};

/// Result of calling the try_next non-blocking API, which obtains the next
/// event descriptor from the reader if it is available and no sequence number
/// gap has occurred
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NextResult {
    Success,  // Reader advanced to next event descriptor
    NotReady, // Next event is not written yet
    Gap,      // Sequence number gap; not advanced
}

/// Holds the iterator state of a single event reader; these are initialized
/// from the EventRing they read from. This is a native Rust reimplementation
/// of the functionality in event_iterator.h, in the C API.
///
/// It is not called EventIterator in Rust, to prevent confusion with the
/// formal Rust iterator concept. This does not implement the Iterator trait,
/// because of the more complex nature of the API: it has a "polling" style
/// and can return a "not ready" indicator, or a gap indicator.
///
/// As in the C API, readers are lightweight and an arbitrary number may
/// exist, but each one is single-threaded.
pub struct EventReader<'ring> {
    descriptors: &'ring [monad_event_descriptor],
    payload_buf: &'ring [u8],
    pub read_last_seqno: u64,
    capacity_mask: usize,
    payload_buf_size_mask: usize,
    pub write_last_seqno: &'ring AtomicU64,
    pub buffer_window_start: &'ring AtomicU64,
}

impl<'ring> EventReader<'ring> {
    /// Initialize a reader of the event ring; each reader has its own state,
    /// and this is called once to initialize that state and set the initial
    /// iteration point
    pub fn new(
        ring: &'ring EventRing,
        ring_type: EventRingType,
        metadata_hash: &'_ [u8; 32],
    ) -> Result<EventReader<'ring>, String> {
        check_event_ring_type(ring, ring_type, metadata_hash)?;
        if ring.c_event_ring.mmap_prot & libc::PROT_READ != libc::PROT_READ {
            return Err(String::from(
                "event ring memory segments are not mapped for reading",
            ));
        }

        let header: &monad_event_ring_header = unsafe { &*ring.c_event_ring.header };
        let descriptors = unsafe {
            std::slice::from_raw_parts(
                ring.c_event_ring.descriptors,
                header.size.descriptor_capacity,
            )
        };
        let payload_buf = unsafe {
            std::slice::from_raw_parts(
                ring.c_event_ring.payload_buf,
                2 * header.size.payload_buf_size,
            )
        };

        let write_last_seqno_ptr: *const u64 = &header.control.last_seqno as *const u64;
        let write_last_seqno: &'ring AtomicU64 = unsafe { &*write_last_seqno_ptr.cast() };

        let buffer_window_start_ptr: *const u64 = &header.control.buffer_window_start as *const u64;
        let buffer_window_start: &'ring AtomicU64 = unsafe { &*buffer_window_start_ptr.cast() };

        Ok(EventReader {
            descriptors,
            payload_buf,
            read_last_seqno: write_last_seqno.load(Ordering::Acquire),
            capacity_mask: header.size.descriptor_capacity - 1,
            payload_buf_size_mask: header.size.payload_buf_size - 1,
            write_last_seqno,
            buffer_window_start,
        })
    }

    /// Copies the next event descriptor into `event` and advances the
    /// iterator, if the next event is available and there is no sequence
    /// number gap; otherwise returns a code indicating why no event
    /// descriptor was copied
    #[inline]
    pub fn try_next(&'_ mut self, event: &'_ mut monad_event_descriptor) -> NextResult {
        let ring_event: &'ring monad_event_descriptor =
            &self.descriptors[(self.read_last_seqno as usize) & self.capacity_mask];
        let ring_seqno_ptr: *const u64 = &ring_event.seqno as *const u64;
        let ring_seqno: &'ring AtomicU64 = unsafe { &*ring_seqno_ptr.cast() };
        let event_seqno = ring_seqno.load(Ordering::Acquire);
        if event_seqno == self.read_last_seqno + 1 {
            *event = *ring_event;
            event.seqno = ring_seqno.load(Ordering::Acquire);
            if event.seqno == event_seqno {
                self.read_last_seqno += 1;
                NextResult::Success
            } else {
                NextResult::Gap
            }
        } else if event_seqno < self.read_last_seqno
            || (event_seqno == self.read_last_seqno && event_seqno == 0)
        {
            NextResult::NotReady
        } else {
            *event = *ring_event;
            NextResult::Gap
        }
    }

    /// Returns Some(T) where T is initialized by the event payload if the
    /// event payload is still valid; otherwise returns None
    #[inline]
    pub fn payload_new<T: Sized>(&'_ mut self, event: &'ring monad_event_descriptor) -> Option<T> {
        let payload: &[u8] = self.payload_peek(event);
        let t: T = unsafe { std::mem::transmute_copy(&*(payload.as_ptr() as *const T)) };
        if self.payload_check(event) {
            Some(t)
        } else {
            None
        }
    }

    /// Obtain a pointer to the event's payload in shared memory in a
    /// zero-copy fashion
    #[inline]
    pub fn payload_peek(&'_ self, event: &'ring monad_event_descriptor) -> &'ring [u8] {
        let payload_begin = (event.payload_buf_offset as usize) & self.payload_buf_size_mask;
        let payload_end = payload_begin + event.payload_size as usize;
        &self.payload_buf[payload_begin..payload_end]
    }

    /// Returns true if the event's payload in shared memory (as returned
    /// by payload_peek) is still valid; if false is returned, the event
    /// payload has been overwritten by a subsequent event
    #[inline]
    pub fn payload_check(&'_ self, event: &'ring monad_event_descriptor) -> bool {
        event.payload_buf_offset >= self.buffer_window_start.load(Ordering::Acquire)
    }

    /// Reset the reader to point to the latest event produced; used for
    /// "hard" gap recovery
    #[inline]
    pub fn reset(&mut self) -> u64 {
        self.read_last_seqno = self.sync_wait();
        self.read_last_seqno
    }

    #[inline]
    fn sync_wait(&'_ mut self) -> u64 {
        let write_last_seqno = self.write_last_seqno.load(Ordering::Acquire);
        if write_last_seqno == 0 {
            self.read_last_seqno = 0;
            self.read_last_seqno
        } else {
            // `write_last_seqno` is atomically incremented before the
            // contents of the associated descriptor table slot (which is
            // `write_last_seqno - 1`) are written. The contents are
            // definitely commited when the sequence number (which is equal to
            // `write_last_seqno`) is atomically stored (with Ordering::Acquire).
            // This waits for that to happen, if it hasn't happened already.
            let index = ((write_last_seqno - 1) as usize) & self.capacity_mask;
            let slot_seqno_ptr: *const u64 = &self.descriptors[index].seqno as *const u64;
            let slot_seqno: &AtomicU64 = unsafe { &*slot_seqno_ptr.cast() };
            while slot_seqno.load(Ordering::Acquire) < write_last_seqno {
                hint::spin_loop();
            }
            write_last_seqno
        }
    }
}

unsafe impl Send for EventReader<'_> {}
