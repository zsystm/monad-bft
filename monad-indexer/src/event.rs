use std::{
    ffi::OsString,
    io::{ErrorKind, Result},
    path::Path,
};

use futures::{Stream, StreamExt};
use inotify::{Inotify, WatchMask};
use monad_block_persist::{is_valid_bft_block_header_path, is_valid_bft_payload_path};
use monad_node_config::{ForkpointConfig, NodeConfig};
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
        // hack because filter_map takes in an impl Future<Option<_>>
        let result = (|| {
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
        })();
        async { result }
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
        // hack because filter_map takes in an impl Future<Option<_>>
        let result = (|| {
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
        })();
        async { result }
    })
}

pub fn node_config_changes(node_config_path: &Path) -> impl Stream<Item = NodeConfig> {
    let init_config = read_node_config(node_config_path).expect("failed to read node_config_path");

    let inotify = Inotify::init().expect("error initializing inotify");
    inotify
        .watches()
        .add(node_config_path, WatchMask::CLOSE_WRITE)
        .expect("failed to watch node_config_path");

    let inotify_buffer = [0; 1024];
    let inotify_events = inotify
        .into_event_stream(inotify_buffer)
        .expect("failed to create inotify event stream");

    let node_config_path = node_config_path.to_path_buf();
    let mut last_config = init_config.clone();
    futures::stream::once(async { init_config }).chain(inotify_events.filter_map(
        move |maybe_event| {
            let node_config_path = node_config_path.clone();
            let last_config = &mut last_config;
            // hack because filter_map takes in an impl Future<Option<_>>
            let result = (|| {
                let event = match maybe_event {
                    Ok(event) => event,
                    Err(err) if err.kind() == ErrorKind::InvalidInput => {
                        warn!(
                        ?err,
                        "ErrorKind::InvalidInput, is node config being changed faster than indexer?"
                    );
                        return None;
                    }
                    Err(err) => {
                        error!(?err, "inotify error while reading events");
                        panic!("inotify error while reading events")
                    }
                };
                assert_eq!(event.name, None);

                let node_config = match read_node_config(&node_config_path) {
                    Ok(node_config) => node_config,
                    Err(err) => {
                        error!(?err, "read_node_config failure");
                        return None;
                    }
                };
                if &node_config == last_config {
                    // deduplicate events
                    return None;
                }
                *last_config = node_config.clone();
                Some(node_config)
            })();
            async { result }
        },
    ))
}

fn read_node_config(node_config_path: &Path) -> Result<NodeConfig> {
    let node_config_raw = std::fs::read_to_string(node_config_path)?;
    let node_config = toml::from_str(&node_config_raw).map_err(std::io::Error::other)?;
    Ok(node_config)
}

/// Pass in path of forkpoint *directory*
pub fn forkpoint_changes(forkpoint_path: &Path) -> impl Stream<Item = ForkpointConfig> {
    let inotify = Inotify::init().expect("error initializing inotify");
    inotify
        .watches()
        .add(forkpoint_path, WatchMask::CLOSE_WRITE)
        .expect("failed to watch node_config_path");

    let inotify_buffer = [0; 1024];
    let inotify_events = inotify
        .into_event_stream(inotify_buffer)
        .expect("failed to create inotify event stream");

    let forkpoint_path = forkpoint_path.to_path_buf();
    let mut last_forkpoint = None;
    inotify_events.filter_map(move |maybe_event| {
        let forkpoint_path = forkpoint_path.clone();
        let last_forkpoint = &mut last_forkpoint;
        // hack because filter_map takes in an impl Future<Option<_>>
        let result = (|| {
            let event = match maybe_event {
                Ok(event) => event,
                Err(err) if err.kind() == ErrorKind::InvalidInput => {
                    warn!(
                        ?err,
                        "ErrorKind::InvalidInput, are forkpoints being generated faster than indexer?"
                    );
                    return None;
                }
                Err(err) => {
                    error!(?err, "inotify error while reading events");
                    panic!("inotify error while reading events")
                }
            };

            let filename = event.name?;
            let mut path_buf = forkpoint_path;
            path_buf.push(filename);

            let forkpoint = match read_forkpoint(&path_buf) {
                Ok(forkpoint) => forkpoint,
                Err(err) => {
                    error!(
                        ?err,
                        ?path_buf,
                        "read_forkpoint failure"
                    );
                    return None;
                }
            };
            if Some(&forkpoint) == last_forkpoint.as_ref() {
                // deduplicate events
                return None;
            }
            *last_forkpoint = Some(forkpoint.clone());
            Some(forkpoint)
        })();
        async { result }
    })
}

fn read_forkpoint(forkpoint_path: &Path) -> Result<ForkpointConfig> {
    let forkpoint_raw = std::fs::read_to_string(forkpoint_path)?;
    let forkpoint = toml::from_str(&forkpoint_raw).map_err(std::io::Error::other)?;
    Ok(forkpoint)
}
