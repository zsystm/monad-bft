# monad-event-ring package

The `monad` execution daemon contains a real-time event notification system
that informs clients about execution progress, e.g., "a new proposed block is
starting execution" or "transaction 13 emitted these 3 logs."

Execution events are communicated using a shared memory broadcast system,
called the "event ring API." A general overview of event rings is provided in
the file `event.md`, in the execution daemon repository. That file also
describes the C API used by client programs that want to listen to events,
which is the basis for this Rust implementation.

As in C, the Rust version is split into two different pieces:

1. __monad-event-ring__ - the core event ring functionality (this package).
   Notably, this package does _not_ include the definitions of the events
   themselves. The event ring library is a generic broadcast notification
   system; it can work with different kinds of event data, not just execution
   events.

2. __monad-exec-events__ - the execution event data types are defined in this
   library. It defines `#[repr(C)]` Rust versions of the execution event data
   structures that are written to shared memory, and some higher level APIs
   that are more useful in native Rust programs.

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

1. Call `std::fs::File::open` to open an event ring shared memory file
2. Call `EventRing::mmap_from_file` to import the event ring into the current
   process
3. Call `EventReader::new` to create a reader of the imported ring
4. Read events from the `EventReader` using its non-blocking API

See the `bin/exec-event-watch.rs` in the `monad-exec-events` package as an
example user of the API.

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
