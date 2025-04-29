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
    last_block_seqno: 44000,
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
