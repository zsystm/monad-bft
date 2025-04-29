//! Utility functions that are useful in most event ring programs

use std::{ffi::CString, path::PathBuf};

use zstd;

use crate::event_ring::*;

/// Validate that an event ring has the expected type and metadata hash
pub fn check_event_ring_type(
    ring: &EventRing,
    ring_type: EventRingType,
    metadata_hash: &'_ [u8; 32],
) -> Result<(), String> {
    let r = unsafe {
        monad_event_ring_check_type(&ring.c_event_ring, ring_type as u16, metadata_hash.as_ptr())
    };
    check_event_ring_library_error(r)
}

/// Find the pids of all processes that have opened the given file descriptor
/// with O_WRONLY or O_RDWR; this is a slow and somewhat brittle operation (it
/// scans the file descriptor tables of all accessible processes in proc(5)),
/// and is typically used to create a more efficient ProcessExitMonitor object,
/// to keep track of each discovered process. For event rings, the intention
/// is to check when all writing processes have died
pub fn find_fd_writer_pids(fd: libc::c_int) -> Result<Vec<libc::pid_t>, String> {
    let mut pids: [libc::pid_t; 32] = [0; 32];
    let mut size: usize = pids.len();
    let r = unsafe { monad_event_ring_find_writer_pids(fd, pids.as_mut_ptr(), &mut size) };
    check_event_ring_library_error(r)?;
    Ok(Vec::from(&pids[..size]))
}

/// A utility that uses the Linux pidfd (see pidfd_open(2)) facility to
/// monitor when a process exits
pub struct ProcessExitMonitor {
    pub pid: libc::pid_t,
    pidfd: libc::c_int,
}

impl ProcessExitMonitor {
    pub fn new(pid: libc::pid_t) -> Result<ProcessExitMonitor, std::io::Error> {
        let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) as libc::c_int };
        if pidfd == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(ProcessExitMonitor { pid, pidfd })
        }
    }

    pub fn has_exited(&self) -> bool {
        let mut pfd = libc::pollfd {
            fd: self.pidfd,
            events: libc::POLLIN,
            revents: 0,
        };
        let r = unsafe { libc::poll(&mut pfd, 1, 0) };
        r == -1 || (pfd.revents & libc::POLLIN) == libc::POLLIN
    }
}

pub fn monitor_single_event_ring_file_writer(
    fd: libc::c_int,
    error_name: &str,
) -> Result<ProcessExitMonitor, String> {
    let writer_pids = find_fd_writer_pids(fd)?;
    let writer_pid = match writer_pids.len() {
        0 => {
            return Err(format!(
                "could not find any processes opening `{error_name}` for writing"
            ))
        }
        1 => writer_pids[0],
        _ => {
            return Err(format!(
                "found multiple processes opening `{error_name}` for writing: {writer_pids:?}"
            ))
        }
    };
    match ProcessExitMonitor::new(writer_pid) {
        Err(e) => Err(e.to_string()),
        Ok(m) => Ok(m),
    }
}

#[cfg(unix)]
pub fn path_supports_hugetlb(path: &std::path::Path) -> Result<bool, String> {
    use std::ffi::CString;
    let path_bytes = path.to_str().ok_or(format!(
        "cannot extract path bytes from `{}`",
        path.display()
    ))?;
    let c_path: CString = match CString::new(path_bytes) {
        Err(e) => return Err(e.to_string()),
        Ok(p) => p,
    };
    let mut supported: bool = false;
    let r = unsafe { monad_check_path_supports_map_hugetlb(c_path.as_ptr(), &mut supported) };
    check_event_ring_library_error(r)?;
    Ok(supported)
}

#[link(name = "monad_event")]
extern "C" {
    fn monad_event_ring_check_type(
        event_ring: *const monad_event_ring,
        ring_type: u16,
        metadata_hash: *const u8,
    ) -> libc::c_int;

    fn monad_event_ring_find_writer_pids(
        ring_fd: libc::c_int,
        pids: *mut libc::pid_t,
        size: *mut libc::size_t,
    ) -> libc::c_int;

    fn monad_check_path_supports_map_hugetlb(
        path: *const libc::c_char,
        supported: *mut bool,
    ) -> libc::c_int;
}

pub struct EventRingSnapshot {
    pub snapshot_fd: libc::c_int,
    pub snapshot_off: libc::off_t,
}

impl EventRingSnapshot {
    pub fn load_from_bytes(bytes: &[u8], error_name: &str) -> EventRingSnapshot {
        let error_name_cstr = CString::new(error_name).unwrap();
        let snapshot_fd: libc::c_int =
            unsafe { libc::memfd_create(error_name_cstr.as_ptr(), libc::MFD_CLOEXEC) };
        assert_ne!(snapshot_fd, -1);
        let snapshot_off: libc::off_t = 0;

        assert!(bytes.len() >= 4);
        let mut magic_bytes = [0u8; 4];
        magic_bytes.copy_from_slice(&bytes[0..4]);
        if u32::from_le_bytes(magic_bytes) == zstd::zstd_safe::MAGICNUMBER {
            let mut decompressed = Vec::new();
            zstd::stream::copy_decode(bytes, &mut decompressed)
                .expect(format!("could not decompress `{error_name}`").as_str());
            let n_write = unsafe {
                libc::write(
                    snapshot_fd,
                    decompressed.as_ptr() as *const libc::c_void,
                    decompressed.len() as libc::size_t,
                )
            };
            assert_eq!(n_write as usize, decompressed.len());
        } else {
            let n_write = unsafe {
                libc::write(
                    snapshot_fd,
                    bytes.as_ptr() as *const libc::c_void,
                    bytes.len() as libc::size_t,
                )
            };
            assert_eq!(n_write as usize, bytes.len());
        };

        EventRingSnapshot {
            snapshot_fd,
            snapshot_off,
        }
    }
}

impl Drop for EventRingSnapshot {
    fn drop(&mut self) {
        unsafe { libc::close(self.snapshot_fd) };
    }
}

pub enum OpenedEventRing {
    Live(EventRing, ProcessExitMonitor),
    Snapshot(EventRing),
}

/// mmap an EventRing from a file which is either a real event ring shared
/// memory file or a zstd-compressed snapshot of one. In the former case,
/// we'll also open a ProcessExitMonitor to the single writer of the event
/// ring.
pub fn open_event_ring_file(event_ring_path: &PathBuf) -> Result<OpenedEventRing, String> {
    let mmap_prot = libc::PROT_READ;
    let error_name = event_ring_path.to_str().unwrap();

    let event_ring_file = match std::fs::File::open(event_ring_path) {
        Err(e) => {
            return Err(format!(
                "open of event ring file `{}` failed: {}",
                event_ring_path.display(),
                e
            ));
        }
        Ok(f) => f,
    };

    // Check if this is a zstd snapshot file
    use std::os::unix::fs::FileExt;
    let mut magic_buf = [0; 4];
    if let Err(e) = event_ring_file.read_exact_at(magic_buf.as_mut_slice(), 0) {
        return Err(format!(
            "failed to read magic number from event ring file `{error_name}`: {e}"
        ));
    }
    if u32::from_le_bytes(magic_buf) == zstd::zstd_safe::MAGICNUMBER {
        let file_bits = std::fs::read(event_ring_path)
            .expect(format!("unable to read contents of zstd file `{error_name}`").as_str());
        let snapshot = EventRingSnapshot::load_from_bytes(file_bits.as_slice(), error_name);
        let event_ring = match EventRing::mmap_from_fd(
            mmap_prot,
            0,
            snapshot.snapshot_fd,
            snapshot.snapshot_off,
            error_name,
        ) {
            Err(e) => return Err(format!("event library error -- {e}")),
            Ok(r) => r,
        };
        return Ok(OpenedEventRing::Snapshot(event_ring));
    }

    // Figure out whether the path specifies a file on a filesystem that
    // supports mmap'ing with MAP_HUGETLB
    let supports_hugetlb = match path_supports_hugetlb(event_ring_path) {
        Err(e) => {
            return Err(format!(
                "cannot determine if event ring file `{error_name}` supports MAP_HUGETLB: {e}"
            ))
        }
        Ok(s) => s,
    };

    let mmap_extra_flags = if supports_hugetlb {
        libc::MAP_POPULATE | libc::MAP_HUGETLB
    } else {
        libc::MAP_POPULATE
    };
    let event_ring = match EventRing::mmap_from_file(
        &event_ring_file,
        mmap_prot,
        mmap_extra_flags,
        0,
        error_name,
    ) {
        Err(e) => return Err(format!("event library error -- {e}")),
        Ok(r) => r,
    };

    // Create an object that can monitor when the associated writer process
    // exits
    use std::os::unix::io::AsRawFd;
    let error_name = event_ring_path.to_str().unwrap();
    let proc_exit_monitor =
        match monitor_single_event_ring_file_writer(event_ring_file.as_raw_fd(), error_name) {
            Err(e) => return Err(format!("event library error -- {e}")),
            Ok(m) => m,
        };

    Ok(OpenedEventRing::Live(event_ring, proc_exit_monitor))
}
