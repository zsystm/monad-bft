use std::{collections::BTreeMap, error::Error, path::PathBuf, pin::pin};

use clap::Parser;
use diesel::{Connection, ExpressionMethods, RunQueryDsl};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use futures::StreamExt;
use monad_block_persist::FileBlockPersist;
use monad_consensus_types::payload::TransactionPayload;
use monad_indexer::{
    create_db_connection,
    event::{forkpoint_changes, new_block_headers, new_block_payloads, node_config_changes},
    models::{validator_set_transform, BlockHeader, BlockPayload, Key, ValidatorSetMember},
};
use monad_node_config::{SignatureCollectionType, SignatureType};
use monad_types::Epoch;
use tracing::{debug, warn};
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

    #[arg(long)]
    pub node_config_path: PathBuf,

    /// Set the *directory* where forkpoints are generated
    #[arg(long)]
    pub forkpoint_path: PathBuf,
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

fn key_writer(key_rx: std::sync::mpsc::Receiver<Vec<Key>>) -> Result<(), Box<dyn Error>> {
    let mut conn = create_db_connection();
    loop {
        let keys = key_rx.recv()?;

        let num_writes = diesel::insert_into(monad_indexer::schema::key::dsl::key)
            .values(&keys)
            .on_conflict_do_nothing()
            .execute(&mut conn)?;
        debug!(?num_writes, "successfully wrote keys");
    }
}

fn validator_set_writer(
    validator_set_rx: std::sync::mpsc::Receiver<Vec<ValidatorSetMember>>,
) -> Result<(), Box<dyn Error>> {
    let mut validator_sets: BTreeMap<Epoch, Vec<ValidatorSetMember>> = Default::default();

    use monad_indexer::schema::validator_set::dsl;
    let mut conn = create_db_connection();
    loop {
        let validator_set = validator_set_rx.recv()?;

        let Some(first) = validator_set.first() else {
            continue;
        };

        let epoch = first.epoch;
        assert!(validator_set
            .iter()
            .all(|validator| validator.epoch == epoch));

        let old_validator_set = validator_sets.get(&Epoch(epoch.try_into().unwrap()));
        if old_validator_set.is_some_and(|old_validator_set| old_validator_set == &validator_set) {
            continue;
        }

        let num_writes = conn.transaction::<_, diesel::result::Error, _>(|conn| {
            diesel::delete(dsl::validator_set)
                .filter(dsl::epoch.eq(epoch))
                .execute(conn)?;
            let num_writes = diesel::insert_into(dsl::validator_set)
                .values(&validator_set)
                .on_conflict_do_nothing()
                .execute(conn)?;
            Ok(num_writes)
        })?;
        debug!(?epoch, ?num_writes, "successfully wrote validator set");

        validator_sets.insert(Epoch(epoch as u64), validator_set);
        while validator_sets.len() > 10 {
            validator_sets.pop_first();
        }
    }
}

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

    let (key_tx, key_rx) = std::sync::mpsc::sync_channel(10);
    std::thread::spawn(move || key_writer(key_rx).expect("key_writer error"));

    let (validator_set_tx, validator_set_rx) = std::sync::mpsc::sync_channel(10);
    std::thread::spawn(move || {
        validator_set_writer(validator_set_rx).expect("validator_set_writer error")
    });

    let mut block_header_paths = pin!(new_block_headers(&cmd.bft_block_header_path));
    let mut block_payload_paths = pin!(new_block_payloads(&cmd.bft_block_header_path));
    let block_persist = FileBlockPersist::<SignatureType, SignatureCollectionType>::new(
        cmd.bft_block_header_path,
        cmd.bft_block_payload_path,
        cmd.execution_ledger_path,
    );

    let mut node_config_changes = pin!(node_config_changes(&cmd.node_config_path));
    let mut forkpoint_changes = pin!(forkpoint_changes(&cmd.forkpoint_path));

    // TODO refine to also index backwards
    loop {
        tokio::select! {
            block_header_path = block_header_paths.next() => {
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
            block_payload_path = block_payload_paths.next() => {
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
            node_config = node_config_changes.next() => {
                let node_config = node_config.expect("node_config stream terminated");
                if let Err(err) = key_tx.try_send(node_config.bootstrap.peers.iter().map(Into::into).collect()) {
                    warn!(
                        ?err,
                        "dropping node_config change, channel full? consider resizing channel"
                    );
                    continue;
                }
            }
            forkpoint = forkpoint_changes.next() => {
                let forkpoint = forkpoint.expect("forkpoint stream terminated");
                for validator_set in forkpoint.validator_sets {
                    if let Err(err) =
                        validator_set_tx.try_send(validator_set_transform(&validator_set))
                    {
                        warn!(
                            ?err,
                            "dropping forkpoint change, channel full? consider resizing channel"
                        );
                        continue;
                    }
                }
            }
        }
    }
}
