use bytes::Bytes;
use eyre::Result;
use enum_dispatch::enum_dispatch;

use super::{memory::MemoryStorage, s3::S3Bucket, KVReader, KVStore, KVStoreErased};

/// Enum for object storage implementations that support range reads.
/// This is used by ModelV2 for reading partial data from S3 references.
#[derive(Clone)]
#[enum_dispatch(KVStore, KVReader)]
pub enum ObjectStore {
    S3(S3Bucket),
    Memory(MemoryStorage),
}

impl ObjectStore {
    /// Get a range of bytes from an object.
    /// The range is inclusive of start and end positions.
    pub async fn get_range(&self, key: &str, start: i32, end: i32) -> Result<Bytes> {
        match self {
            ObjectStore::S3(s3) => s3.get_range(key, start, end).await,
            ObjectStore::Memory(mem) => mem.get_range(key, start, end).await,
        }
    }
}

impl Into<KVStoreErased> for ObjectStore {
    fn into(self) -> KVStoreErased {
        match self {
            ObjectStore::S3(s3) => KVStoreErased::S3Bucket(s3),
            ObjectStore::Memory(mem) => KVStoreErased::MemoryStorage(mem),
        }
    }
}

// impl KVReader for ObjectStore {
//     async fn get(&self, key: &str) -> Result<Option<Bytes>> {
//         match self {
//             ObjectStore::S3(s3) => s3.get(key).await,
//             ObjectStore::Memory(mem) => mem.get(key).await,
//         }
//     }
// }

// impl KVStore for ObjectStore {
//     async fn put(&self, key: impl AsRef<str>, data: Vec<u8>) -> Result<()> {
//         match self {
//             ObjectStore::S3(s3) => s3.put(key, data).await,
//             ObjectStore::Memory(mem) => mem.put(key, data).await,
//         }
//     }

//     async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
//         match self {
//             ObjectStore::S3(s3) => s3.scan_prefix(prefix).await,
//             ObjectStore::Memory(mem) => mem.scan_prefix(prefix).await,
//         }
//     }

//     async fn delete(&self, key: impl AsRef<str>) -> Result<()> {
//         match self {
//             ObjectStore::S3(s3) => s3.delete(key).await,
//             ObjectStore::Memory(mem) => mem.delete(key).await,
//         }
//     }

//     fn bucket_name(&self) -> &str {
//         match self {
//             ObjectStore::S3(s3) => s3.bucket_name(),
//             ObjectStore::Memory(mem) => mem.bucket_name(),
//         }
//     }
// }
