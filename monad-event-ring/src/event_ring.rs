//! Definition of the EventRing object and the shared memory structures within
//! an event ring

use std::{
    ffi::{CStr, CString},
    sync::atomic::AtomicU64,
};

/// Describes which event ring an event is recorded to; different categories
/// of events are recorded to different rings; this must match the C enumeration
/// constant `enum monad_event_ring_type` in event_ring.h
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u16)]
pub enum EventRingType {
    Invalid, // Invalid value
    Test,    // Test events
    Exec,    // Core execution events
    Count,   // Maximum known value
}

/// Rust wrapper around the underlying C `monad_event_ring` object, which is
/// a shared memory event ring mapped into our process address space
pub struct EventRing {
    pub(crate) c_event_ring: monad_event_ring,
}

/// This type associates the contents of an event ring's shared memory mappings
/// with safe, Rust-native types; all reference fields point into the ring's
/// shared memory segments, so the associated reference lifetime ('ring) is
/// taken from the EventRing object whose lifetime is holding the memory
/// mapping in place (the mappings are removed by EventRing::drop)
pub(crate) struct EventRingContents<'ring> {
    pub(crate) descriptors: &'ring [monad_event_descriptor],
    pub(crate) payload_buf: &'ring [u8],
    pub(crate) descriptor_capacity: usize,
    pub(crate) payload_buf_size: usize,
    pub(crate) write_last_seqno: &'ring AtomicU64,
    pub(crate) buffer_window_start: &'ring AtomicU64,
}

impl EventRing {
    /// Given a file descriptor to a file containing an event ring, mmap(2)
    /// the event ring shared memory into our process' address space
    #[cfg(unix)]
    pub fn mmap_from_fd(
        mmap_prot: libc::c_int,
        mmap_extra_flags: libc::c_int,
        ring_fd: libc::c_int,
        ring_offset: libc::off_t,
        error_name: &str,
    ) -> Result<Self, String> {
        let mut c_event_ring: monad_event_ring = unsafe { std::mem::zeroed::<monad_event_ring>() };
        let error_name_cstr: CString = match CString::new(error_name) {
            Err(_) => {
                return Err(String::from(
                    "error_name argument contained an embedded nul",
                ))
            }
            Ok(s) => s,
        };
        let r = unsafe {
            monad_event_ring_mmap(
                &mut c_event_ring,
                mmap_prot,
                mmap_extra_flags,
                ring_fd,
                ring_offset,
                error_name_cstr.as_ptr(),
            )
        };
        check_event_ring_library_error(r)?;
        Ok(EventRing { c_event_ring })
    }

    /// Convenience function for calling mmap_from_fd using a File object
    #[cfg(unix)]
    pub fn mmap_from_file(
        file: &std::fs::File,
        mmap_prot: libc::c_int,
        mmap_extra_flags: libc::c_int,
        ring_offset: libc::off_t,
        error_name: &str,
    ) -> Result<Self, String> {
        use std::os::unix::io::AsRawFd;
        Self::mmap_from_fd(
            mmap_prot,
            mmap_extra_flags,
            file.as_raw_fd() as libc::c_int,
            ring_offset,
            error_name,
        )
    }

    /// Returned as the metadata hash for an invalid event ring
    pub const ZERO_HASH: [u8; 32] = [0; 32];

    /// Returns what kind of event ring this is, along with the SHA-256 hash
    /// of the event metadata, indicating the binary version of the event
    /// schema
    pub fn get_type(&self) -> (EventRingType, &[u8; 32]) {
        let rt = unsafe { &(*self.c_event_ring.header) }.event_ring_type;
        if rt >= EventRingType::Count as u16 {
            return (EventRingType::Invalid, &Self::ZERO_HASH);
        };
        let header: &monad_event_ring_header = unsafe { &(*self.c_event_ring.header) };
        (
            unsafe { std::mem::transmute::<u16, EventRingType>(rt) },
            &header.metadata_hash,
        )
    }

    /// Return a raw pointer to the context area; what this contains depends
    /// on the event ring type
    pub fn get_context_area(&self) -> *const libc::c_void {
        self.c_event_ring.context_area
    }

    pub(crate) fn get_contents(&self) -> EventRingContents {
        let header: &monad_event_ring_header = unsafe { &*self.c_event_ring.header };
        let descriptors = unsafe {
            std::slice::from_raw_parts(
                self.c_event_ring.descriptors,
                header.size.descriptor_capacity,
            )
        };
        let payload_buf = unsafe {
            std::slice::from_raw_parts(
                self.c_event_ring.payload_buf,
                2 * header.size.payload_buf_size,
            )
        };

        let write_last_seqno_ptr: *const u64 = &header.control.last_seqno as *const u64;
        let write_last_seqno = unsafe { AtomicU64::from_ptr(write_last_seqno_ptr as *mut u64) };

        let buffer_window_start_ptr: *const u64 = &header.control.buffer_window_start as *const u64;
        let buffer_window_start =
            unsafe { AtomicU64::from_ptr(buffer_window_start_ptr as *mut u64) };

        EventRingContents {
            descriptors,
            payload_buf,
            descriptor_capacity: header.size.descriptor_capacity,
            payload_buf_size: header.size.payload_buf_size,
            write_last_seqno,
            buffer_window_start,
        }
    }

    pub(crate) fn get_mmap_prot(&self) -> libc::c_int {
        self.c_event_ring.mmap_prot
    }
}

impl Drop for EventRing {
    fn drop(&mut self) {
        unsafe {
            monad_event_ring_unmap(&mut self.c_event_ring);
        }
    }
}

/// Descriptor for a single event; this fixed-size object describes the common
/// attributes of an event, and is passed between threads via a shared memory
/// ring buffer (the threads are potentially in different processes). The
/// variably-sized extra content of the event (specific to each event type) is
/// called the "event payload", and lives in a shared memory segment called the
/// "payload buffer", which can be accessed using this descriptor
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct monad_event_descriptor {
    pub seqno: u64,
    pub event_type: u16,
    pub unused: u16,
    pub payload_size: u32,
    pub record_epoch_nanos: u64,
    pub payload_buf_offset: u64,
    pub user: [u64; 4],
}

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct monad_event_ring_header {
    pub magic: [u8; 6],
    pub event_ring_type: u16,
    pub metadata_hash: [u8; 32],
    pub size: monad_event_ring_size,
    pub control: monad_event_ring_control,
}

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct monad_event_ring_size {
    pub descriptor_capacity: usize,
    pub payload_buf_size: usize,
    pub context_area_size: usize,
}

#[allow(non_camel_case_types)]
#[repr(C, align(64))]
pub struct monad_event_ring_control {
    pub last_seqno: u64,
    pub next_payload_byte: u64,
    align64_padding: [u8; 48],
    pub buffer_window_start: u64,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Copy)]
#[repr(C)]
pub(crate) struct monad_event_ring {
    pub(crate) mmap_prot: libc::c_int,
    pub(crate) header: *const monad_event_ring_header,
    pub(crate) descriptors: *const monad_event_descriptor,
    pub(crate) payload_buf: *const u8,
    pub(crate) context_area: *const libc::c_void,
}

/*
/// Array of human-readable names for the event ring types
extern char const *g_monad_event_ring_type_names[MONAD_EVENT_RING_TYPE_COUNT];
 */
// Declaration of public interface from event_ring.h and event_ring_util.h;
// for now, we only declare the interface needed to be a reader of event rings
#[link(name = "monad_event")]
extern "C" {
    fn monad_event_ring_mmap(
        event_ring: *mut monad_event_ring,
        mmap_prot: libc::c_int,
        mmap_extra_flags: libc::c_int,
        ring_fd: libc::c_int,
        ring_offset: libc::off_t,
        error_name: *const libc::c_char,
    ) -> libc::c_int;

    fn monad_event_ring_unmap(event_ring: *mut monad_event_ring);

    fn monad_event_ring_get_last_error() -> *const libc::c_char;

    static g_monad_event_ring_type_names: *const *const libc::c_char;
}

pub(crate) fn check_event_ring_library_error(rc: libc::c_int) -> Result<(), String> {
    if rc != 0 {
        let err_str = unsafe {
            CStr::from_ptr(monad_event_ring_get_last_error())
                .to_str() // Convert to a &str
                .unwrap_or("invalid UTF-8 in monad_event_ring_get_last_error?")
        };
        Err(String::from(err_str))
    } else {
        Ok(())
    }
}
