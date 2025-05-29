# monad-exec-events package

The `monad-event-ring` package adds support for the event ring shared memory
communication system. This is a low-latency "broadcast" style notification
API. That package, however, does not define the event data types that are
inserted into the ring buffer. The event ring is a generic data structure,
and the events themselves are defined in separate Rust packages.

This Rust package defines the execution events. An execution event is a
notification that the Ethereum Virtual Machine has performed some action,
such as emitting a transaction log or changing an account balance. These
events are produced by the execution daemon, a separate process running on
the same host.

## What's in this package?

### Rust modules

- __exec_event_ctypes__ - defines the C layout compatible execution event types
- __eth_ctypes__ - types in `exec_event_ctypes` that are entirely Ethereum-related
  are factored out into this module, in case having a C-layout compatible
  definition of these types is useful in other contexts
- __exec_events__ - defines higher level "Rust native" versions of the types
  from `exec_event_ctypes.rs`; the latter are more appropriate for zero-copy
  `unsafe` usage, whereas these are for idiomatic Rust programs
- __exec_event_stream__ - defines a utility for constructing the Rust native
  types from the from their "raw" C versions in shared memory; the stream
  takes an `EventReader` from the `monad-event-ring` package as input, and
  produces a stream of `ExecEvent` objects as output
- __block_builder__ - aggregates fine-grained events into block-at-a-time
  updates
- __consensus_state_tracker__ - tracks a proposal as it moves through various
  consensus states; the primary benefit of this utility is that it explicitly
  announces when a proposal becomes abandoned. At the event level, abandonment
  does not happen explicitly; rather, abandonment of a proposal is implied by
  the finalization of another proposal competing for the same block number
- __exec_event_test_util__ - defines utilities and reference data used for
  testing; it is located in `src` and not `test` so that downstream libraries
  that listen to execution events can reuse the publicly-exported testing
  infrastructure of this package

### Rust CLI programs

- __exec-event-watch__ - a debugging program that prints event contents to
  stdout (and which doubles as a simple API example). It operates in one
  of two modes, exposed through two different CLI subcommands:
  - _Raw mode_ uses the low-level API: it directly calls the event ring API
    to iterate through events in shared memory; it can be used to check
    "what's really on the wire"
  - _Cooked mode_ uses the `ExecEventStream` to build the higher level Rust
    native types
- __blockwatch__ - uses the `exec_event_stream`, `block_builder` and
  `consensus_state_tracker` modules to print detailed, block-at-a-time
  updates to stdout
- __event-cvt-cli__ - a command line program used to run a full system
  test called the "event cross-validation test" (CVT). A description of
  this test appears in the documentation comment of the
  `CrossValidationTestUpdate` type, in the `exec_event_test_util.rs` module

### Other files in this package

- __Test data__ - the directory `src/test_data` contains reference binary data
  used by test suites; it is loaded by the utilities in the
  `exec_event_test_util` module
