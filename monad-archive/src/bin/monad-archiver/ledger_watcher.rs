use std::{
    collections::HashSet,
    ffi::OsString,
    path::{Path, PathBuf},
    time::Duration,
};

use eyre::{Context, Result};
use futures::StreamExt;
use monad_archive::BlobStoreErased;
use tokio::time::sleep;
use tracing::{debug, error, info};

use crate::{BlobStore, Metrics};

const BFT_BLOCK_PREFIX: &'static str = "bft_block/";

pub async fn parallel_upload_iter(
    uploaded: &mut HashSet<OsString>,
    s3: BlobStoreErased,
    dir: PathBuf,
    fnames: Vec<OsString>,
) -> usize {
    futures::stream::iter(fnames)
        .filter_map(|fname| {
            let s3 = s3.clone();
            let dir = dir.clone();
            spawning_upload(s3, dir, fname)
        })
        .buffer_unordered(20)
        .map(|block_hash_uploaded| {
            info!(?block_hash_uploaded, "Uploaded bft block");
            uploaded.insert(block_hash_uploaded);
            futures::future::ready(())
        })
        .count()
        .await
}

async fn spawning_upload(
    s3: BlobStoreErased,
    dir: PathBuf,
    block_hash_os_str: OsString,
) -> Option<futures::future::Ready<OsString>> {
    let r = tokio::spawn(upload(s3, dir, block_hash_os_str))
        .await
        .ok()?;

    r
}

async fn upload(
    s3: BlobStoreErased,
    dir: PathBuf,
    block_hash_os_str: OsString,
) -> Option<futures::future::Ready<OsString>> {
    let block_hash = block_hash_os_str.to_str()?;
    let mut path = PathBuf::from(&dir);
    path.push(&block_hash_os_str);

    let data = match tokio::fs::read(path).await {
        Ok(x) => x,
        Err(e) => {
            error!(
                ?e,
                block_hash, "Failed to read bft block file from ledger folder"
            );
            return None;
        }
    };

    match s3
        .upload(&format!("{BFT_BLOCK_PREFIX}{block_hash}"), data)
        .await
    {
        Ok(_) => Some(futures::future::ready(block_hash_os_str)),
        Err(e) => {
            error!(?e, block_hash, "Failed to archive bft block");
            None
        }
    }
}

pub async fn ledger_watcher_worker(
    s3: BlobStoreErased,
    dir: PathBuf,
    poll_frequency: Duration,
    metrics: Metrics,
) -> Result<()> {
    let mut uploaded = initialize_uploaded_set(&s3)
        .await
        .wrap_err("Failed to fetch set of already uploaded bft blocks")?;

    loop {
        sleep(poll_frequency).await;
        info!("Checking for bft blocks to upload...");

        let to_upload = match find_unuploaded_blocks(&mut uploaded, &dir).await {
            Ok(x) => x,
            Err(e) => {
                error!(?e, dir = ?dir.as_os_str(), "Failed to scan ledger directory for bft blocks, sleeping...");
                continue;
            }
        };
        let num_to_upload = to_upload.len();
        if num_to_upload == 0 {
            continue;
        }
        info!(num_to_upload, "Found bft blocks to upload");

        let num_uploaded =
            parallel_upload_iter(&mut uploaded, s3.clone(), dir.clone(), to_upload).await;

        if num_uploaded > 0 {
            info!(num_uploaded, num_to_upload, "Uploaded bft blocks");
        } else {
            info!(num_to_upload)
        }
        metrics.gauge("bft_blocks_uploaded", uploaded.len() as u64);
    }
}

async fn find_unuploaded_blocks(uploaded: &HashSet<OsString>, dir: &Path) -> Result<Vec<OsString>> {
    let mut to_upload = Vec::new();
    let mut read_dir = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let meta = entry.metadata().await?;
        let fname = entry.file_name();
        if meta.is_file() && fname != "wal" && !uploaded.contains(&fname) {
            debug!(?fname, "Found file to upload");
            to_upload.push(fname);
        } else {
            debug!(?fname, "Found file, won't upload");
        }
    }
    Ok(to_upload)
}

async fn initialize_uploaded_set(s3: &impl BlobStore) -> Result<HashSet<OsString>> {
    Ok(s3
        .scan_prefix(BFT_BLOCK_PREFIX)
        .await?
        .into_iter()
        .map(|object| {
            let block_hash = object.trim_start_matches(BFT_BLOCK_PREFIX);
            OsString::from(block_hash)
        })
        .collect())
}
