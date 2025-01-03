use std::{collections::HashSet, ffi::OsString, path::PathBuf, time::Duration};

use eyre::Result;
use futures::StreamExt;
use tokio_retry::Retry;
use tracing::error;

use crate::{retry_strategy, BlobStore, S3Bucket};

pub async fn ledger_watcher_worker(s3: S3Bucket) -> Result<()> {
    let dir = "ledger";
    let uploaded: HashSet<OsString> = HashSet::new();
    let mut to_upload: Vec<OsString>;

    loop {
        to_upload.clear();
        for entry in std::fs::read_dir(dir)? {
            let Ok(entry) = entry else {
                continue;
            };

            let meta = entry.metadata()?;
            let fname = entry.file_name();
            if meta.is_file() && fname != "wal" && !uploaded.contains(&fname) {
                to_upload.push(fname);
            }
        }

        futures::stream::iter(to_upload)
            .map(|block_hash_os_str| async move {
                let block_hash = block_hash_os_str.to_str()?;
                let mut path = PathBuf::from(dir);
                path.push(&block_hash_os_str);
                let Ok(data) = std::fs::read(path) else {
                    error!(
                        block_hash,
                        "Failed to read bft block file from ledger folder"
                    );
                    return std::option::Option::<()>::None;
                };

                match s3.upload(&format!("bft_block/{block_hash}"), data).await {
                    Ok(_) => {
                        uploaded.insert(block_hash_os_str);
                    }
                    Err(e) => error!(block_hash, "Failed to archive bft block: {e:?}"),
                };

                None
            })
            .buffer_unordered(20)
            .count()
            .await;
    }

    todo!()
}

fn setup_watcher() {}
