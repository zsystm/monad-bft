//! Utility functions that are useful in most event ring programs

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
