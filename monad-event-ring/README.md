# monad-event-ring package

The `monad` execution daemon contains a real-time event notification system
that informs clients about execution progress, e.g., "a new proposed block is
starting execution" or "transaction 13 emitted these 3 logs."

Execution events are communicated using a shared memory broadcast system,
called the "event ring API." A general overview of the system is provided in
the file `event.md`, in the execution daemon repository. That file also
describes the C API used by client programs that want to listen to events,
which is the basis for this Rust API.

## What's in this package?

- __Library__ - the `monad-event-ring` library allows Rust programs to act as
  readers of execution events. It is a (mostly) Rust-native reimplementation
  of the C client library described in `libs/event/doc/event.md`
- __exec-event-watch__ - `src/bin/exec-event-watch.rs` is a sample program that
  shows how the API is used, and is also a debugging tool that can dump
  execution events to `stdout`
- __Test data__ - the directory `src/test_data` contains reference binary data
  used by test suites; it is located in `src` and not `test` because downstream
  libraries that listen to execution events reuse the publicly-exported testing
  infrastructure of this package

### Other execution event packages

Execution events are emitted by `monad` as soon as they are _committed_ to a
proposed block. "Committed" is used in the sense that the execution daemon
means it, i.e., at `BlockState::merge` time: it refers to an intra-block
timeframe, and is _not_ related to the concept of a block being appended to
the blockchain.

A block can potentially contain thousands of events: every interesting thing
that happens at the transaction level (e.g., every individual account balance
update) is broadcast as a separate event during the lifetime of that block's
execution. This makes execution events _timely_ but potentially unwieldy.

In downstream packages, utilities are provided which reassemble these "low
level" execution events into block-oriented updates. Those utilities will better
suit your needs if block-at-a-time updates are more convenient for your
use-case. That said, these utilities also require an `EventReader` (defined in
this package) as input, so you will still need to use the initial setup
functions in this API.

## API usage overview

There are two important objects in the API, listed in the order you create
them:

2. `EventRing`: an event ring which has been imported into our process. This
   object's lifetime corresponds to the lifetime of the event ring's shared
   memory mappings that were added to our address space by the import. In
   other words, the call to `EventRing::drop` will remove the shared memory
   mappings from our process.

3. `EventReader<'ring>`: used to read events from an event ring. This type is
   similar in spirit to an iterator, but with a poll-like interface instead of
   a `next`-based one: it must report states such as the next event not yet
   being ready, or that a sequence number gap occurred. You can create as many
   readers as you want, but each one is single-threaded. The EventReader is
   parameterized by a `'ring` reference lifetime. This lifetime comes from
   a reference to the associated `EventRing`: the reader points into the
   shared memory region whose memory mapping is tied to the lifetime of the
   `EventRing` instance.

The general flow of the API:

1. Call `std::fs::File::open` to open an event ring shared memory file. This
   will usually be the default name of particular event ring, e.g., for
   the execution event ring, it is
   `exec_event_ring_metadata::EXEC_EVENT_DEFAULT_RING_PATH`
2. Call `EventRing::mmap_from_file` to import the event ring into the current
   process
3. Call `EventReader::new` to create a reader of the imported ring
4. Read events from the `EventReader` using its non-blocking API

See the `bin/exec-event-watch.rs` example program to see all these steps in
action

## Implementation details

### Rust vs C APIs

- The "reader" functionality corresponds to the header-only C interface
  `monad_event_iterator.h`; this a simple (~100 lines of code) and very
  performance sensitive part of the library, so the Rust version is a
  native reimplementation; no C code is reused
- The event ring interface from `monad_event_ring.h` is wrapped in Rust,
  but the C library does the actual work via `extern "C"` calls;
  consequently, we must build and link the C library `libmonad_event.a`

### Correspondence between C and Rust source files

C interface             | Rust module              | Purpose                                                                            | Rust impl. strategy
----------------------- |--------------------------|------------------------------------------------------------------------------------| ---
`event_iterator.h`      | `event_reader.rs`        | Interface for reading events; renamed in Rust because it's not a formal Iterator   | Rust native
`event_metadata.h`      | `event_metadata.rs`      | Interface for metadata about event rings and event types                           | Rust native
`event_ring.h`          | `event_ring.rs`          | API for importing shared memory segments into a reader process                     | Rust wrapper around C
`exec_event_types.h`    | `exec_event_types.rs`    | All execution-specific payload data structures and the execution event enum type   | Generated code
`exec_event_metadata.h` | `exec_event_metadata.rs` | All execution-specific metadata, including the metadata hash and default ring path | Generated code
