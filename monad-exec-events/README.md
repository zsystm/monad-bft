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
- __exec_event_test_util__ - defines utilities and reference data used for
  testing; it is located in `src` and not `test` so that downstream libraries
  that listen to execution events can reuse the publicly-exported testing
  infrastructure of this package

### Rust CLI programs

- __exec-event-watch__ - a debugging program that prints event contents to
  stdout (and which doubles as a simple API example)

### Other files in this package

- __Test data__ - the directory `src/test_data` contains reference binary data
  used by test suites; it is loaded by the utilities in the
  `exec_event_test_util` module
