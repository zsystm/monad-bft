//! Utility for printing monad execution events to stdout
//!
//! Given the slow performance of string formatting, this exists primarily as
//! an example of how to use the monad-event-ring library, and as a debugging
//! utility to check that everything is working.

use std::{
    io::{Cursor, Seek, Write},
    path::PathBuf,
    process,
    sync::atomic::Ordering,
};

use chrono::{Local, TimeZone};
use clap::Parser;
use monad_event_ring::{
    event_metadata::*,
    event_reader::*,
    event_ring::*,
    event_ring_util::ProcessExitMonitor,
    event_test_util::{open_event_ring_file, OpenedEventRing},
    exec_event_types::{self, exec_event_type},
    exec_event_types_metadata::{EXEC_EVENT_DEFAULT_RING_PATH, EXEC_EVENT_DOMAIN_METADATA},
};

#[derive(Parser)]
#[command(about = "Utility for watching monad execution events")]
struct Cli {
    #[arg(short, long, help = "name of the event ring shared memory file")]
    #[arg(default_value = EXEC_EVENT_DEFAULT_RING_PATH)]
    event_ring_path: PathBuf,

    #[arg(long, help = "display a hexdump of event payloads")]
    hexdump: bool,

    #[arg(short, long, help = "display a decoded view of event strings")]
    decode: bool,
}

fn print_event(
    reader: &mut EventReader,
    event: &monad_event_descriptor,
    metadata_table: &[EventMetadata],
    block_header_table: &[exec_event_types::block_header],
    hexdump_payload: bool,
    decode_payload: bool,
    fmtcursor: &mut Cursor<&mut [u8]>,
    stdout_handle: &mut std::io::StdoutLock,
) {
    // Print a summary line of this event
    // <HH:MM::SS.nanos> <event-c-name> [<event-type> <event-type-hex>]
    //     SEQ: <sequence-no> LEN: <payload-size>
    let event_time_tz = Local
        .timestamp_nanos(event.record_epoch_nanos as i64)
        .format("%H:%M:%S.%9f");

    // Unpack the event metadata
    let event_seqno = event.seqno;
    let event_code = event.event_type as usize;
    let event_meta = &metadata_table[event_code];
    let event_name = event_meta.c_name;
    let event_size = event.payload_size;
    let event_buf_offset = event.payload_buf_offset;

    // Format the fields present for all events
    let mut event_line = format!(
        "{event_time_tz} {event_name} [{event_code} \
{event_code:#x}] SEQ: {event_seqno} LEN: {event_size} BUF_OFF: {event_buf_offset}"
    );

    let flow_info =
        unsafe { std::mem::transmute::<u64, exec_event_types::flow_info>(event.user[0]) };

    // Add extra information for events associated with blocks and transactions
    match (flow_info.block_flow_id, flow_info.txn_id) {
        // Event is not associated with a block
        (0, 0) => event_line.push('\n'),

        // Top-level block-related event (pertaining to the whole block but
        // not to any particular transaction, e.g., the BLOCK_START event).
        // Print the summary block info
        (block_flow_id, 0) => {
            let block_header = &block_header_table[block_flow_id as usize];
            let (block_number, round) = (block_header.exec_input.number, block_header.round);
            event_line.push_str(format!(" BLK: {block_number} [R: {round}]\n").as_str())
        }

        // Event is associated with a transaction in a block; print both
        // summary block info and the transaction number
        (block_flow_id, txn_id) => {
            let block_header = &block_header_table[block_flow_id as usize];
            let (block_number, round) = (block_header.exec_input.number, block_header.round);
            let txn_num = txn_id - 1;
            event_line
                .push_str(format!(" BLK: {block_number} [R: {round}] TXN: {txn_num}\n").as_str())
        }
    }
    let _ = stdout_handle.write(event_line.as_bytes());
    let payload: &[u8] = reader.payload_peek(event);

    // Format a hexdump of the event payload
    if hexdump_payload {
        let _ = fmtcursor.rewind();
        for (line, line_bytes) in payload.chunks(16).enumerate() {
            // Print one line of the dump, which is 16 bytes, in the form:
            // <offset> <8 byte chunk> <8 byte chunk>
            let _ = write!(fmtcursor, "{:#08x} ", line * 16);
            let mut c = line_bytes.chunks(8);
            if let Some(bytes) = c.next() {
                for b in bytes {
                    let _ = write!(fmtcursor, "{:02x}", b);
                }
            }
            if let Some(bytes) = c.next() {
                let _ = write!(fmtcursor, " ");
                for b in bytes {
                    let _ = write!(fmtcursor, "{:02x}", b);
                }
            }
            let _ = writeln!(fmtcursor);

            // Every 512 bytes (32 16-byte lines), check if the payload page data
            // is still valid; the initial line bias avoids checking on the first
            // iteration
            if (line + 1) % 32 == 0 && !reader.payload_check(event) {
                break; // Escape to the end, which checks the final time
            }
        }

        if !reader.payload_check(event) {
            eprintln!("ERROR: event {event_seqno} payload lost!");
        } else {
            let _ = stdout_handle.write(&fmtcursor.get_ref()[..fmtcursor.position() as usize]);
        }
    }

    if decode_payload {
        // TODO(ken): make this show trailing data, like the C++ version does
        let event_type = unsafe { std::mem::transmute::<u16, exec_event_type>(event.event_type) };
        let s = exec_event_types::format_as(payload, event_type);
        if !reader.payload_check(event) {
            eprintln!("ERROR: event {event_seqno} payload lost!");
        } else {
            println!("{s}");
        }
    }
}

// Main event loop: once we've successfully imported a shared memory event
// ring, poll it for new events and dump them to stdout until the writer
// process dies
fn event_loop(
    reader: &mut EventReader,
    metadata_table: &[EventMetadata],
    block_header_table: &[exec_event_types::block_header],
    hexdump_payload: bool,
    decode_payload: bool,
    opt_proc_exit_monitor: Option<&ProcessExitMonitor>,
) {
    let mut not_ready_count: u64 = 0;
    let mut event: monad_event_descriptor = monad_event_descriptor::default();
    let stdout = std::io::stdout();
    let mut stdout_handle = stdout.lock();

    // Create a pre-allocated buffer for print_event to use for its hexdump
    // formatting; we'd ideally do this with a thread_local in print_event
    // itself, but the Rust thread_local seems needlessly heavy-weight
    let mut fmtbuf = vec![0u8; 1 << 25].into_boxed_slice();
    let mut fmtcursor: Cursor<&mut [u8]> = Cursor::new(fmtbuf.as_mut());

    // Poll the event ring for the next event (i.e., a non-blocking reading)
    loop {
        match reader.try_next(&mut event) {
            NextResult::NotReady => {
                // No event is ready. Usually we'll just try again, but we'll
                // keep track of how many times in a row we're getting
                // "NotReady". If it happens often enough, we'll check if the
                // process is dead. This is a slow operation: it requires a
                // poll(2) system call, so we don't want to do it every time
                not_ready_count += 1;
                if not_ready_count & ((1 << 20) - 1) == 0 {
                    let _ = stdout_handle.flush();
                    if opt_proc_exit_monitor.is_some()
                        && opt_proc_exit_monitor.unwrap().has_exited()
                    {
                        break;
                    }
                }
            }
            NextResult::Gap => {
                // We didn't consume events fast enough and lost some
                not_ready_count = 0;
                eprintln!(
                    "ERROR: event gap from {} to {}",
                    reader.read_last_seqno,
                    reader.write_last_seqno.load(Ordering::Acquire)
                );
                reader.reset();
                continue;
            }
            NextResult::Success => {
                // An event is ready, print it
                not_ready_count = 0;
                print_event(
                    reader,
                    &event,
                    metadata_table,
                    block_header_table,
                    hexdump_payload,
                    decode_payload,
                    &mut fmtcursor,
                    &mut stdout_handle,
                )
            }
        }
    }
}

fn main() {
    let cli = Cli::parse();

    let event_ring;
    let opt_proc_exit_monitor;
    let is_snapshot;

    // Open the event ring shared memory file
    match open_event_ring_file(&cli.event_ring_path) {
        Err(e) => {
            eprintln!("{e}");
            process::exit(libc::EXIT_FAILURE);
        }
        Ok(o) => match o {
            OpenedEventRing::Live(r, p) => {
                event_ring = r;
                opt_proc_exit_monitor = Some(p);
                is_snapshot = false;
            }
            OpenedEventRing::Snapshot(r) => {
                event_ring = r;
                opt_proc_exit_monitor = None;
                is_snapshot = true;
            }
        },
    }

    let mut event_reader = match EventReader::new(
        &event_ring,
        EventRingType::Exec,
        &EXEC_EVENT_DOMAIN_METADATA.metadata_hash,
    ) {
        Err(e) => {
            eprintln!("event library error -- {e}");
            process::exit(libc::EXIT_FAILURE);
        }
        Ok(r) => r,
    };

    if is_snapshot {
        event_reader.read_last_seqno = 0;
    }

    let block_header_table = unsafe {
        std::slice::from_raw_parts(
            event_ring.get_context_area() as *const exec_event_types::block_header,
            4096,
        )
    };

    event_loop(
        &mut event_reader,
        EXEC_EVENT_DOMAIN_METADATA.events,
        block_header_table,
        cli.hexdump,
        cli.decode,
        opt_proc_exit_monitor.as_ref(),
    );
}
