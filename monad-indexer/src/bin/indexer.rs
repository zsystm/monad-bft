use std::{
    error::Error,
    ffi::OsString,
    io::ErrorKind,
    path::{Path, PathBuf},
    pin::pin,
};

use clap::Parser;
use diesel::RunQueryDsl;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use futures::{Stream, StreamExt};
use monad_block_persist::FileBlockPersist;
use monad_bls::BlsSignatureCollection;
use monad_consensus_types::payload::TransactionPayload;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_indexer::{
    create_db_connection,
    event::{new_block_headers, new_block_payloads},
    models::{BlockHeader, BlockPayload},
};
use monad_secp::SecpSignature;
use tracing::{debug, error, warn};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

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

        let num_writes =
            diesel::insert_into(monad_indexer::schema::block_header::dsl::block_header)
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

        let num_writes =
            diesel::insert_into(monad_indexer::schema::block_payload::dsl::block_payload)
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
    let mut block_payload_paths = pin!(new_block_payloads(&cmd.bft_block_header_path));
    let block_persist = FileBlockPersist::<SignatureType, SignatureCollectionType>::new(
        cmd.bft_block_header_path,
        cmd.bft_block_payload_path,
        cmd.execution_ledger_path,
    );

    // TODO refine to also index backwards
    loop {
        let event =
            futures::future::select(block_header_paths.next(), block_payload_paths.next()).await;
        match event {
            futures::future::Either::Left((block_header_path, _)) => {
                let block_header_path = block_header_path.expect("block_header stream terminated");

                debug!(?block_header_path, "reading block header");
                let maybe_block_header =
                    block_persist.read_bft_block_from_filepath(&block_header_path);
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
            }
            futures::future::Either::Right((block_payload_path, _)) => {
                let block_payload_path =
                    block_payload_path.expect("block_payload stream terminated");

                debug!(?block_payload_path, "reading block payload");
                let block_payload =
                    match block_persist.read_bft_payload_from_filepath(&block_payload_path) {
                        Ok(block_payload) => block_payload,
                        Err(err) => {
                            warn!(?block_payload_path, ?err, "failed to read block payload");
                            continue;
                        }
                    };
                debug!(?block_payload_path, "successfully read block payload");

                if let TransactionPayload::Null = block_payload.txns {
                    debug!(?block_payload_path, "dropping null block payload");
                    continue;
                }

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
}
