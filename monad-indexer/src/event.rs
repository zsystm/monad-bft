use std::{ffi::OsString, io::ErrorKind, path::Path};

use futures::{Stream, StreamExt};
use inotify::{Inotify, WatchMask};
use monad_block_persist::{is_valid_bft_block_header_path, is_valid_bft_payload_path};
use tracing::{error, warn};

pub fn new_block_headers(bft_block_header_path: &Path) -> impl Stream<Item = OsString> {
    let inotify = Inotify::init().expect("error initializing inotify");
    inotify
        .watches()
        .add(bft_block_header_path, WatchMask::CLOSE_WRITE)
        .expect("failed to watch bft_block_header_path");

    let inotify_buffer = [0; 1024];
    let inotify_events = inotify
        .into_event_stream(inotify_buffer)
        .expect("failed to create inotify event stream");

    let bft_block_header_path = bft_block_header_path.to_path_buf();
    inotify_events.filter_map(move |maybe_event| {
        let bft_block_header_path = bft_block_header_path.clone();
        async move {
            let event = match maybe_event {
                Ok(event) => event,
                Err(err) if err.kind() == ErrorKind::InvalidInput => {
                    warn!(
                        ?err,
                        "ErrorKind::InvalidInput, are blocks being produced faster than indexer?"
                    );
                    return None;
                }
                Err(err) => {
                    error!(?err, "inotify error while reading events");
                    panic!("inotify error while reading events")
                }
            };
            let filename = event.name?;
            if is_valid_bft_block_header_path(&filename) {
                let mut path_buf = bft_block_header_path;
                path_buf.push(filename);
                Some(path_buf.into())
            } else {
                None
            }
        }
    })
}

pub fn new_block_payloads(bft_block_payload_path: &Path) -> impl Stream<Item = OsString> {
    let inotify = Inotify::init().expect("error initializing inotify");
    inotify
        .watches()
        .add(bft_block_payload_path, WatchMask::CLOSE_WRITE)
        .expect("failed to watch bft_block_payload_path");

    let inotify_buffer = [0; 1024];
    let inotify_events = inotify
        .into_event_stream(inotify_buffer)
        .expect("failed to create inotify event stream");

    let bft_block_payload_path = bft_block_payload_path.to_path_buf();
    inotify_events.filter_map(move |maybe_event| {
        let bft_block_payload_path = bft_block_payload_path.clone();
        async move {
            let event = match maybe_event {
                Ok(event) => event,
                Err(err) if err.kind() == ErrorKind::InvalidInput => {
                    warn!(
                        ?err,
                        "ErrorKind::InvalidInput, are blocks being produced faster than indexer?"
                    );
                    return None;
                }
                Err(err) => {
                    error!(?err, "inotify error while reading events");
                    panic!("inotify error while reading events")
                }
            };
            let filename = event.name?;
            if is_valid_bft_payload_path(&filename) {
                let mut path_buf = bft_block_payload_path;
                path_buf.push(filename);
                Some(path_buf.into())
            } else {
                None
            }
        }
    })
}
