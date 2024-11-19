use std::{sync::Arc, time::Duration};

use crate::cli_base::{S3Args, StorageType};
use crate::errors::ArchiveError;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region};
use aws_sdk_s3::{primitives::ByteStream, Client};
use dashmap::DashMap;
use eyre::{bail, eyre, Context};
use futures::{Stream, StreamExt};
use std::path::PathBuf;
use tokio::fs::{create_dir_all, read, write};
use tokio::sync::Semaphore;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use tracing::{error, warn};

pub trait KVStore: Clone + Sync + Send + 'static {
    fn upload(
        &self,
        key: &str,
        value: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<(), ArchiveError>> + Send;

    fn read(
        &self,
        key: &str,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, ArchiveError>> + Send;

    async fn upload_batch<'a>(
        &self,
        keys: impl Iterator<Item = String>,
        data: Vec<u8>,
    ) -> Result<(), ArchiveError> {
        for key in keys {
            self.upload(&key, data.clone()).await?;
        }
        Ok(())
    }
}

pub struct Store(Arc<StoreInner>);

pub enum StoreInner {
    Memory(MemoryStore),
    S3(S3Store),
    File(FileStore),
}

impl KVStore for Store {
    async fn upload(&self, key: &str, value: Vec<u8>) -> Result<(), ArchiveError> {
        match self.0.as_ref() {
            StoreInner::Memory(store) => store.upload(key, value).await,
            StoreInner::S3(store) => store.upload(key, value).await,
            StoreInner::File(store) => store.upload(key, value).await,
        }
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, ArchiveError> {
        match self.0.as_ref() {
            StoreInner::Memory(store) => store.read(key).await,
            StoreInner::S3(store) => store.read(key).await,
            StoreInner::File(store) => store.read(key).await,
        }
    }

    async fn upload_batch<'a>(
        &self,
        keys: impl Iterator<Item = String>,
        data: Vec<u8>,
    ) -> Result<(), ArchiveError> {
        match self.0.as_ref() {
            StoreInner::Memory(store) => store.upload_batch(keys, data).await,
            StoreInner::S3(store) => store.upload_batch(keys, data).await,
            StoreInner::File(store) => store.upload_batch(keys, data).await,
        }
    }
}

pub async fn kv_store_from_args(args: StorageType) -> Result<Store, ArchiveError> {
    let store = match args {
        StorageType::S3(s3) => {
            let store = S3Store::new(s3).await?;
            StoreInner::S3(store)
        }
        StorageType::Memory => StoreInner::Memory(MemoryStore::default()),
        StorageType::Fs { dir } => {
            let store = FileStore::new(dir).await?;
            StoreInner::File(store)
        }
    };

    Ok(Store(Arc::new(store)))
}

impl Clone for Store {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

// todo: consider removing clone here so we can rate limit all client reqs centrally
// this would allow removing the semaphores and simplify other code
#[derive(Clone)]
pub struct S3Store {
    pub client: Client,
    pub bucket: String,
    pub semaphore: Arc<Semaphore>,
    /// must still use semaphore, but can be used as an upper bound
    pub max_concurrent_connections: usize,
}

impl S3Store {
    pub async fn new(args: S3Args) -> Result<Self, ArchiveError> {
        let region_provider = RegionProviderChain::default_provider().or_else(
            args.region
                .map(Region::new)
                .unwrap_or_else(|| Region::new("us-east-2")),
        );
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;
        let client = Client::new(&config);
        Ok(S3Store {
            client,
            bucket: args.bucket,
            semaphore: Arc::new(Semaphore::new(args.max_concurrent_connections)),
            max_concurrent_connections: args.max_concurrent_connections,
        })
    }
}

impl KVStore for S3Store {
    // Upload rlp-encoded bytes with retry
    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<(), ArchiveError> {
        let retry_strategy = ExponentialBackoff::from_millis(10)
            .max_delay(Duration::from_secs(1))
            .map(jitter);

        let _permit = self.semaphore.acquire().await?;

        Retry::spawn(retry_strategy, move || {
            let client = &self.client;
            let bucket = &self.bucket;
            let key = key.to_string();
            let body = ByteStream::from(data.clone());

            async move {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(&key)
                    .body(body)
                    .send()
                    .await
                    .map_err(|e| {
                        warn!("Failed to upload {}: {}. Retrying...", key, e);
                        eyre!("Failed to upload {}: {}", key, e)
                    })
            }
        })
        .await
        .map(|_| ())
        .context(format!("Failed to upload after retries {}", key))
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, ArchiveError> {
        let resp = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => output,
            Err(e) => {
                bail!("Fail to read from S3: {}", e);
            }
        };

        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| eyre!("Unable to collect response data: {:?}", e))?;
        Ok(data.to_vec())
    }

    async fn upload_batch<'a>(
        &self,
        keys: impl Iterator<Item = String>,
        data: Vec<u8>,
    ) -> Result<(), ArchiveError> {
        let _ = futures::stream::iter(keys)
            .map(|key| {
                let data = data.clone();
                async move { self.upload(&key, data).await }
            })
            .buffer_unordered(self.max_concurrent_connections)
            .count()
            .await;
        Ok(())
    }
}

#[derive(Clone)]
pub struct FileStore {
    root_dir: PathBuf,
}

impl FileStore {
    pub async fn new(dir: PathBuf) -> Result<Self, ArchiveError> {
        // TODO: consider restricting this to only creating 1 directory deep, but not failing if it exists
        create_dir_all(&dir)
            .await
            .wrap_err("Failed to create directory")?;
        Ok(FileStore { root_dir: dir })
    }

    fn key_to_path(&self, key: &str) -> PathBuf {
        self.root_dir.join(key)
    }
}

impl KVStore for FileStore {
    async fn upload(&self, key: &str, value: Vec<u8>) -> Result<(), ArchiveError> {
        let path = self.key_to_path(key);
        if let Some(parent) = path.parent() {
            create_dir_all(parent)
                .await
                .wrap_err("Failed to create parent directory")?;
        }
        write(&path, value)
            .await
            .wrap_err("Failed to write file {}: {}")
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, ArchiveError> {
        let path = self.key_to_path(key);
        read(&path).await.wrap_err("Failed to read file {}: {}")
    }
}

#[derive(Default, Clone)]
pub struct MemoryStore(pub Arc<DashMap<String, Vec<u8>>>);

impl KVStore for MemoryStore {
    fn upload(
        &self,
        key: &str,
        value: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<(), ArchiveError>> + Send {
        async {
            self.0.insert(key.into(), value);
            Ok(())
        }
    }

    fn read(
        &self,
        key: &str,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, ArchiveError>> + Send {
        async move {
            let Some(res) = self.0.get(key) else {
                bail!("Failed to read key: {key}");
            };
            let res: &Vec<u8> = &res;
            Ok(res.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn test_memory_store() {
        let store = MemoryStore::default();
        test_basic_operations(store).await;
    }

    #[tokio::test]
    async fn test_fs_store() {
        let test_dir = std::env::temp_dir().join("kvstore_test");
        let _ = fs::remove_dir_all(&test_dir); // Cleanup from previous runs

        let store = FileStore::new(test_dir.clone()).await.unwrap();
        test_basic_operations(store).await;

        // Persistence test
        let key = "persist";
        let value = vec![5, 5, 5];
        let store = FileStore::new(test_dir.clone()).await.unwrap();
        store.upload(key, value.clone()).await.unwrap();

        let store2 = FileStore::new(test_dir.clone()).await.unwrap();
        assert_eq!(store2.read(key).await.unwrap(), value);

        fs::remove_dir_all(test_dir).unwrap();
    }
}

async fn test_basic_operations<S: KVStore>(store: S) {
    // Simple key/value
    let key = "test_key";
    let value = vec![1, 2, 3, 4];
    store.upload(key, value.clone()).await.unwrap();
    assert_eq!(store.read(key).await.unwrap(), value);

    // Empty value
    let empty_value = vec![];
    store.upload("empty", empty_value.clone()).await.unwrap();
    assert_eq!(store.read("empty").await.unwrap(), empty_value);

    // Large value
    let large_value = vec![42; 1024 * 1024]; // 1MB
    store.upload("large", large_value.clone()).await.unwrap();
    assert_eq!(store.read("large").await.unwrap(), large_value);

    // Overwrite
    let value1 = vec![1, 1, 1];
    let value2 = vec![2, 2, 2];
    store.upload("overwrite", value1).await.unwrap();
    store.upload("overwrite", value2.clone()).await.unwrap();
    assert_eq!(store.read("overwrite").await.unwrap(), value2);

    // Nonexistent key
    assert!(store.read("nonexistent").await.is_err());

    // Empty key
    store.upload("", vec![1]).await.unwrap();
    assert_eq!(store.read("").await.unwrap(), vec![1]);
}
