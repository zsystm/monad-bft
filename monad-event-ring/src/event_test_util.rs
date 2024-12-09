//! Utility for creating a "fake" (i.e., not really in shared memory)
//! EventRing file that contains a fixed set of events

use std::{ffi::CString, path::PathBuf, process};

use zstd;

use crate::{
    event_ring::EventRing,
    event_ring_util::{
        monitor_single_event_ring_file_writer, path_supports_hugetlb, ProcessExitMonitor,
    },
};

pub struct EventRingSnapshot {
    pub snapshot_fd: libc::c_int,
    pub snapshot_off: libc::off_t,
}

impl EventRingSnapshot {
    pub fn load_from_zstd_bytes(zstd_bytes: &[u8], error_name: &str) -> EventRingSnapshot {
        let error_name_cstr = CString::new(error_name).unwrap();
        let snapshot_fd: libc::c_int =
            unsafe { libc::memfd_create(error_name_cstr.as_ptr(), libc::MFD_CLOEXEC) };
        assert_ne!(snapshot_fd, -1);
        let snapshot_off: libc::off_t = 0;

        let mut decompressed = Vec::new();
        zstd::stream::copy_decode(zstd_bytes, &mut decompressed)
            .expect(format!("could not decompress `{error_name}`").as_str());

        let n_write = unsafe {
            libc::write(
                snapshot_fd,
                decompressed.as_ptr() as *const libc::c_void,
                decompressed.len() as libc::size_t,
            )
        };
        assert_eq!(n_write as usize, decompressed.len());

        EventRingSnapshot {
            snapshot_fd,
            snapshot_off,
        }
    }

    pub fn load_from_scenario(scenario: &ExecEventTestScenario) -> EventRingSnapshot {
        Self::load_from_zstd_bytes(scenario.event_ring_snapshot_zst, scenario.name)
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
        let snapshot = EventRingSnapshot::load_from_zstd_bytes(file_bits.as_slice(), error_name);
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
        Err(e) => {
            eprintln!("event library error -- {e}");
            process::exit(libc::EXIT_FAILURE);
        }
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

/// Each instance of this object references a collection of binary data
/// that represents a particular test case scenario for the execution event
/// system
pub struct ExecEventTestScenario<'a> {
    /// Human-readable name of the scenario
    pub name: &'a str,

    /// A compressed snapshot of an event ring file, compressed with zstd; we
    /// can decompress this to mock up the shared memory contents as they
    /// appeared during a real run of the execution daemon (from which this
    /// snapshot was taken)
    pub event_ring_snapshot_zst: &'a [u8],

    /// The expected values of the SpeculativeBlockUpdate objects that would be
    /// produced by the execution event stream captured in the associated
    /// `event_ring_snapshot`. Specifically, this is a JSON serialized (and
    /// zstd compressed) array of SpeculativeBlockUpdate objects. The data is
    /// produced by the execution daemon, which directly serializes its own
    /// internal data structures in the format used by SpeculativeBlockUpdate
    /// via the serde_json package.
    ///
    /// Note that `struct SpeculativeBlockUpdate` is not part of this library:
    /// it is defined in a downstream package (monad-rpc), which uses this
    /// library as a dependency. We choose to put its test data here because
    /// the two pieces of data are part of the same scenario (these are
    /// precisely the expected updates that correspond to the low-level events
    /// in the event ring snapshot), so they are kept together in a single
    /// object that represents the scenario itself. This field is not used by
    /// any `monad-event-ring` package tests, as it related to downstream
    /// functionality.
    pub expected_block_update_json_zst: &'a [u8],

    /// Sequence number of the last BLOCK_FINALIZE event in the event ring
    /// snapshot
    pub last_block_seqno: u64,
}

// The file "exec-events-emn-30b-15m.zst" contains the execution events
// produced by the execution daemon during a historical replay of the
// Ethereum mainnet (emn) for a run of 30 blocks (30b), starting at block
// 15,000,000 (15m). The file "expected-block-updates-emn-30b-15m.json.zst"
// contains the expected block updates as exported by the execution daemon
// from that same sequence of events.
pub const ETHEREUM_MAINNET_30B_15M: ExecEventTestScenario = ExecEventTestScenario {
    name: "ETHEREUM_MAINNET_30B_15M",
    event_ring_snapshot_zst: include_bytes!("test_data/exec-events-emn-30b-15m.zst"),
    expected_block_update_json_zst: include_bytes!(
        "test_data/expected-block-updates-emn-30b-15m.json.zst"
    ),
    last_block_seqno: 21541,
};

// The file "exec-events-mdn-21b-genesis.zst" contains the execution events
// produced by the execution daemon during a test sequence on the Monad
// devnet (mdn) for a run of 21 blocks (21b), starting from the genesis
// block. This sequence is notable for its presence of duplicate proposals
// and proposals which do not become finalized.
// TODO(ken): we should use reference data that's easy to regenerate; this
//    was hand-crafted for me by Aashish, and is not part of the record of
//    any public blockchain
pub const MONAD_DEVNET_21B_GENESIS: ExecEventTestScenario = ExecEventTestScenario {
    name: "MONAD_DEVNET_21B_GENESIS",
    event_ring_snapshot_zst: include_bytes!("test_data/exec-events-mdn-21b-genesis.zst"),
    expected_block_update_json_zst: include_bytes!(
        "test_data/expected-block-updates-mdn-21b-genesis.json.zst"
    ),
    last_block_seqno: 73350,
};
