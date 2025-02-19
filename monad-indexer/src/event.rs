use std::{
    io::{ErrorKind, Result},
    path::Path,
};

use futures::{Stream, StreamExt};
use inotify::{Inotify, WatchMask};
use monad_block_persist::BLOCKDB_HEADER_EXTENSION;
use monad_node_config::NodeConfig;
use monad_types::{BlockId, Hash};
use tracing::{error, warn};

pub fn new_block_headers(bft_block_header_path: &Path) -> impl Stream<Item = BlockId> {
    let inotify = Inotify::init().expect("error initializing inotify");
    inotify
        .watches()
        .add(bft_block_header_path, WatchMask::CLOSE_WRITE)
        .expect("failed to watch bft_block_header_path");

    let inotify_buffer = [0; 1024];
    let inotify_events = inotify
        .into_event_stream(inotify_buffer)
        .expect("failed to create inotify event stream");

    inotify_events.filter_map(move |maybe_event| {
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
            if filename
                .to_str()
                .is_some_and(|f| f.ends_with(BLOCKDB_HEADER_EXTENSION))
            {
                let filename = filename.to_str().unwrap();
                let block_id =
                    hex::decode(filename.strip_suffix(BLOCKDB_HEADER_EXTENSION).unwrap())
                        .expect("unable to decode block header id");
                Some(BlockId(Hash(
                    block_id.try_into().expect("block_header_id not 32 bytes"),
                )))
            } else {
                None
            }
        })();
        async move { result }
    })
}

pub fn node_config_changes(
    node_config_path: &Path,
) -> impl Stream<Item = NodeConfig<monad_secp::PubKey>> {
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

fn read_node_config(node_config_path: &Path) -> Result<NodeConfig<monad_secp::PubKey>> {
    let node_config_raw = std::fs::read_to_string(node_config_path)?;
    let node_config = toml::from_str(&node_config_raw).map_err(std::io::Error::other)?;
    Ok(node_config)
}
