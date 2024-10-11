use std::{
    error::Error,
    ffi::OsString,
    io::ErrorKind,
    path::{Path, PathBuf},
    pin::pin,
};

use clap::Parser;
use diesel::{Connection, PgConnection, RunQueryDsl};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use futures::{Stream, StreamExt};
use inotify::{Inotify, WatchMask};
use models::{BlockHeader, BlockPayload};
use monad_block_persist::{is_valid_bft_block_header_path, BlockPersist, FileBlockPersist};
use monad_bls::BlsSignatureCollection;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_secp::SecpSignature;
use tracing::{debug, error, warn};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

pub mod models;
pub mod schema;

#[derive(Debug, Parser)]
#[command(name = "monad-indexer", about, long_about = None)]
pub struct Cli {
    /// Set the path where the execution ledger will be stored
    #[arg(long)]
    pub execution_ledger_path: PathBuf,

    /// Set the path where the bft block headers will be stored
    #[arg(long)]
    pub bft_block_header_path: PathBuf,

    /// Set the path where the bft block payloads will be stored
    #[arg(long)]
    pub bft_block_payload_path: PathBuf,
}

fn new_block_headers(bft_block_header_path: &Path) -> impl Stream<Item = OsString> {
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

fn create_db_connection() -> PgConnection {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL not set");
    PgConnection::establish(&database_url)
        .unwrap_or_else(|_| panic!("error connecting to {}", database_url))
}

fn block_header_writer(
    block_header_rx: std::sync::mpsc::Receiver<BlockHeader>,
) -> Result<(), Box<dyn Error>> {
    let mut conn = create_db_connection();
    loop {
        // block on first header
        let block_header = block_header_rx.recv()?;

        // read any remaining headers that may be ready
        let block_headers: Vec<_> = std::iter::once(block_header)
            .chain(block_header_rx.try_iter())
            .collect();

        let num_writes = diesel::insert_into(schema::block_header::dsl::block_header)
            .values(&block_headers)
            .on_conflict_do_nothing()
            .execute(&mut conn)?;
        debug!(?num_writes, "successfully wrote block headers");
    }
}

fn block_payload_writer(
    block_payload_rx: std::sync::mpsc::Receiver<BlockPayload>,
) -> Result<(), Box<dyn Error>> {
    let mut conn = create_db_connection();
    loop {
        // block on first payload
        let block_payload = block_payload_rx.recv()?;

        // read any remaining payloads that may be ready
        let block_payloads: Vec<_> = std::iter::once(block_payload)
            .chain(block_payload_rx.try_iter())
            .collect();

        let num_writes = diesel::insert_into(schema::block_payload::dsl::block_payload)
            .values(&block_payloads)
            .on_conflict_do_nothing()
            .execute(&mut conn)?;
        debug!(?num_writes, "successfully wrote block payloads");
    }
}

type SignatureType = SecpSignature;
type SignatureCollectionType = BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let cmd = Cli::parse();

    {
        let mut conn = create_db_connection();
        conn.run_pending_migrations(MIGRATIONS)
            .expect("failed to run migrations");
    }

    let (block_header_tx, block_header_rx) = std::sync::mpsc::sync_channel(10);
    std::thread::spawn(move || {
        block_header_writer(block_header_rx).expect("block_header_writer error")
    });

    let (block_payload_tx, block_payload_rx) = std::sync::mpsc::sync_channel(10);
    std::thread::spawn(move || {
        block_payload_writer(block_payload_rx).expect("block_payload_writer error")
    });

    let mut block_header_paths = pin!(new_block_headers(&cmd.bft_block_header_path));
    let block_persist = FileBlockPersist::<SignatureType, SignatureCollectionType>::new(
        cmd.bft_block_header_path,
        cmd.bft_block_payload_path,
        cmd.execution_ledger_path,
    );

    // TODO refine to also index backwards
    loop {
        let block_header_path = block_header_paths
            .next()
            .await
            .expect("block_header stream terminated");

        debug!(?block_header_path, "reading block header");
        let maybe_block_header = block_persist.read_bft_block_from_filepath(&block_header_path);
        let block_header = match maybe_block_header {
            Ok(block_header) => block_header,
            Err(err) => {
                warn!(?block_header_path, ?err, "failed to read block header");
                continue;
            }
        };
        debug!(
            ?block_header_path,
            ?block_header,
            "successfully read block header"
        );

        if let Err(err) = block_header_tx.try_send((&block_header).into()) {
            warn!(
                ?err,
                "dropping block header, channel full? consider resizing channel"
            );
            continue;
        }

        if !block_header.is_empty_block() {
            // we don't writes payloads for null blocks

            let payload_id = &block_header.payload_id;
            let block_payload = match block_persist.read_bft_payload(payload_id) {
                Ok(block_payload) => block_payload,
                Err(err) => {
                    warn!(
                        ?payload_id,
                        ?block_header,
                        ?err,
                        "failed to read block payload"
                    );
                    continue;
                }
            };
            debug!(
                ?payload_id,
                ?block_header,
                "successfully read block payload"
            );

            if let Err(err) = block_payload_tx.try_send((&block_payload).into()) {
                warn!(
                    ?err,
                    "dropping block payload, channel full? consider resizing channel"
                );
                continue;
            }
        }
    }
}
