use crate::*;
use alloy_rlp::{Decodable, Encodable};
use bytes::Bytes;
use eyre::Result;
use eyre::{Context, ContextCompat};
use rocksdb::DB;
use std::sync::Arc;
use std::{collections::HashMap, path::Path};

#[derive(Clone)]
pub struct RocksDbClient {
    pub db: Arc<DB>,
    pub name: String,
}

impl RocksDbClient {
    pub fn new(path: impl AsRef<Path>) -> Result<RocksDbClient> {
        Ok(RocksDbClient {
            db: Arc::new(DB::open_default(path.as_ref())?),
            name: format!(
                "rocksdb://{}",
                path.as_ref()
                    .to_str()
                    .wrap_err_with(|| format!("Path not valid utf-8. {:?}", path.as_ref()))?
            ),
        })
    }
}

impl BlobStore for RocksDbClient {
    fn bucket_name(&self) -> &str {
        &self.name
    }

    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.db.put(key, data).map_err(Into::into)
    }

    async fn read(&self, key: &str) -> Result<Bytes> {
        self.db
            .get(key)?
            .wrap_err_with(|| format!("Key not found in rocks_db: {key}"))
            .map_err(Into::into)
            .map(Into::into)
    }
}

// NOTE: we're doing blocking io here even though we're in async. This should be moved to
// spawn blocking if this code will be used in rpc
impl IndexStore for RocksDbClient {
    async fn bulk_put(&self, kvs: impl Iterator<Item = TxIndexedData>) -> Result<()> {
        for data in kvs {
            let key = data.tx.hash();
            let mut rlp_data = Vec::with_capacity(4096);
            data.encode(&mut rlp_data);
            self.db
                .put(key, rlp_data)
                .wrap_err_with(|| format!("Failed to write tx data to index: {key}"))?;
        }
        self.db.flush()?;
        Ok(())
    }

    async fn bulk_get(&self, keys: &[String]) -> Result<HashMap<String, TxIndexedData>> {
        // NOTE: This is an unfortunate amount of cloning going on...
        let mut results = HashMap::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = self.get(key.clone()).await? {
                results.insert(key.into(), value);
            }
        }
        Ok(results)
    }

    async fn get(&self, key: impl Into<String>) -> Result<Option<TxIndexedData>> {
        let Some(data) = self.db.get(key.into())? else {
            return Ok(None);
        };

        Ok(Some(TxIndexedData::decode(&mut data.as_slice())?))
    }
}
