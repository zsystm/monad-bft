use std::{collections::HashMap, path::Path, sync::Arc};

use alloy_primitives::TxHash;
use alloy_rlp::{Decodable, Encodable};
use bytes::Bytes;
use eyre::{Context, ContextCompat, OptionExt, Result};
use rocksdb::DB;
use tokio::sync::Mutex;
use tracing::info;

use crate::*;

#[derive(Clone)]
pub struct MemoryStorage {
    pub db: Arc<Mutex<HashMap<String, Bytes>>>,
    pub index: Arc<Mutex<HashMap<TxHash, TxIndexedData>>>,
    pub name: String,
}

impl MemoryStorage {
    pub fn new(name: String) -> MemoryStorage {
        MemoryStorage {
            db: Arc::new(Mutex::new(HashMap::default())),
            index: Arc::new(Mutex::new(HashMap::default())),
            name,
        }
    }
}

impl BlobReader for MemoryStorage {
    async fn read(&self, key: &str) -> Result<Bytes> {
        self.db
            .lock()
            .await
            .get(key)
            .ok_or_eyre("Key not found")
            .map(ToOwned::to_owned)
    }
}

impl BlobStore for MemoryStorage {
    fn bucket_name(&self) -> &str {
        &self.name
    }

    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.db.lock().await.insert(key.to_owned(), data.into());
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .db
            .lock()
            .await
            .keys()
            .filter_map(|k| {
                if k.starts_with(prefix) {
                    Some(k.to_owned())
                } else {
                    None
                }
            })
            .collect())
    }
}

impl IndexStoreReader for MemoryStorage {
    async fn bulk_get(&self, keys: &[TxHash]) -> Result<HashMap<TxHash, TxIndexedData>> {
        // NOTE: This is an unfortunate amount of cloning going on...
        let mut results = HashMap::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = self.get(key).await? {
                results.insert(*key, value);
            }
        }
        Ok(results)
    }

    async fn get(&self, key: &TxHash) -> Result<Option<TxIndexedData>> {
        Ok(self.index.lock().await.get(key).map(ToOwned::to_owned))
    }
}

// NOTE: we're doing blocking io here even though we're in async. This should be moved to
// spawn blocking if this code will be used in rpc
impl IndexStore for MemoryStorage {
    async fn bulk_put(&self, kvs: impl Iterator<Item = TxIndexedData>) -> Result<()> {
        for data in kvs {
            let key = data.tx.tx_hash();
            self.index.lock().await.insert(*key, data);
        }
        Ok(())
    }
}
