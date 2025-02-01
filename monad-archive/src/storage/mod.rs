pub mod cloud_proxy;
pub mod dynamodb;
pub mod memory;
pub mod rocksdb_storage;
pub mod s3;
pub mod triedb_reader;

use std::collections::HashMap;

use alloy_primitives::BlockHash;
use alloy_rlp::{RlpDecodable, RlpEncodable};
use bytes::Bytes;
pub use cloud_proxy::*;
pub use dynamodb::*;
use enum_dispatch::enum_dispatch;
use eyre::Result;
use futures::future::try_join_all;
use memory::MemoryStorage;
use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};
pub use rocksdb_storage::*;
pub use s3::*;
use serde::{Deserialize, Serialize};
use tokio_retry::strategy::{jitter, ExponentialBackoff};

use crate::{
    block_data_archive::BlockDataArchive,
    prelude::*,
    storage::{triedb_reader::TriedbReader, DynamoDBArchive},
};

#[enum_dispatch(BlockDataReader)]
#[derive(Clone)]
pub enum BlockDataReaderErased {
    BlockDataArchive,
    TriedbReader,
}

#[enum_dispatch(KVStore, KVReader)]
#[derive(Clone)]
pub enum KVStoreErased {
    RocksDbClient,
    S3Bucket,
    DynamoDBArchive,
    MemoryStorage,
}

#[enum_dispatch(KVReader)]
#[derive(Clone)]
pub enum KVReaderErased {
    RocksDbClient,
    S3Bucket,
    MemoryStorage,
    DynamoDBArchive,
    CloudProxyReader,
}

impl From<KVStoreErased> for KVReaderErased {
    fn from(value: KVStoreErased) -> Self {
        match value {
            KVStoreErased::RocksDbClient(x) => KVReaderErased::RocksDbClient(x),
            KVStoreErased::S3Bucket(x) => KVReaderErased::S3Bucket(x),
            KVStoreErased::MemoryStorage(x) => KVReaderErased::MemoryStorage(x),
            KVStoreErased::DynamoDBArchive(x) => KVReaderErased::DynamoDBArchive(x),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
pub struct RangeRlp {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
pub struct TxByteOffsets {
    pub tx: RangeRlp,
    pub receipt: RangeRlp,
    pub trace: RangeRlp,
}

pub struct BlockDataWithOffsets {
    pub block: Block,
    pub receipts: Vec<ReceiptWithLogIndex>,
    pub traces: Vec<Vec<u8>>,
    pub offsets: Option<Vec<TxByteOffsets>>,
}

#[enum_dispatch]
pub trait BlockDataReader: Clone {
    fn get_bucket(&self) -> &str;
    async fn get_block_data_with_offsets(&self, block_num: u64) -> Result<BlockDataWithOffsets>;
    async fn get_latest(&self, latest_kind: LatestKind) -> Result<u64>;
    async fn get_block_by_number(&self, block_num: u64) -> Result<Block>;
    async fn get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Block>;
    async fn get_block_receipts(&self, block_number: u64) -> Result<Vec<ReceiptWithLogIndex>>;
    async fn get_block_traces(&self, block_number: u64) -> Result<Vec<Vec<u8>>>;
}

#[enum_dispatch]
pub trait KVStore: KVReader {
    async fn put(&self, key: impl AsRef<str>, data: Vec<u8>) -> Result<()>;
    async fn bulk_put(&self, kvs: impl IntoIterator<Item = (String, Vec<u8>)>) -> Result<()> {
        futures::stream::iter(kvs)
            .map(|(k, v)| self.put(k, v))
            .buffer_unordered(10)
            .count()
            .await;
        Ok(())
    }
    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>>;
    fn bucket_name(&self) -> &str;
}

#[enum_dispatch]
pub trait KVReader: Clone {
    async fn get(&self, key: &str) -> Result<Option<Bytes>>;
    async fn bulk_get(&self, keys: &[String]) -> Result<HashMap<String, Bytes>> {
        // Note: a stream based approach runs into lifetime generality errors for some reason here.
        // After a lot of variations I could not get it to work, so fell back on this join_all approach even
        // though it involves an extra allocation.
        // Optimize at your own risk!
        let mut futs = Vec::with_capacity(keys.len());
        for key in keys {
            let reader = self.clone();
            futs.push(async move { reader.get(key).await });
        }
        let responses = try_join_all(futs).await?;

        let mut out = HashMap::with_capacity(responses.len());
        for (resp, key) in responses.into_iter().zip(keys) {
            // let resp = resp?;
            if let Some(bytes) = resp {
                out.insert(key.clone(), bytes);
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub struct TxIndexedData {
    pub tx: TxEnvelopeWithSender,
    pub trace: Vec<u8>,
    pub receipt: ReceiptWithLogIndex,
    pub header_subset: HeaderSubset,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, RlpEncodable, RlpDecodable,
)]
#[rlp(trailing)]
pub struct HeaderSubset {
    pub block_hash: BlockHash,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub tx_index: u64,
    pub gas_used: u128,
    pub base_fee_per_gas: Option<u64>,
}

pub fn retry_strategy() -> std::iter::Map<ExponentialBackoff, fn(Duration) -> Duration> {
    ExponentialBackoff::from_millis(10)
        .max_delay(Duration::from_secs(1))
        .map(jitter)
}
