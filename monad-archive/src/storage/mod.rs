pub mod cloud_proxy;
pub mod dynamodb;
pub mod fs_storage;
pub mod memory;
pub mod rocksdb_storage;
pub mod s3;
pub mod triedb_reader;

use std::{collections::HashMap, str::FromStr};

use alloy_consensus::{ReceiptEnvelope, TxEnvelope};
use alloy_primitives::{BlockHash, TxHash};
use alloy_rlp::{RlpDecodable, RlpEncodable};
use bytes::Bytes;
use clap::{Parser, Subcommand};
pub use cloud_proxy::*;
pub use dynamodb::*;
use enum_dispatch::enum_dispatch;
use eyre::{bail, ContextCompat, OptionExt, Result};
use fs_storage::FsStorage;
use futures::FutureExt;
use memory::MemoryStorage;
pub use rocksdb_storage::*;
pub use s3::*;
use serde::{Deserialize, Serialize};
use tokio::{join, try_join};

use crate::{
    triedb_reader::TriedbReader,
    workers::block_data_archive::{Block, BlockDataArchive},
    ArchiveReader, LatestKind, Metrics, TxIndexArchiver,
};

#[enum_dispatch]
pub trait BlobStore: BlobReader {
    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<()>;

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>>;
    fn bucket_name(&self) -> &str;
}

#[enum_dispatch]
pub trait IndexStore: IndexStoreReader {
    async fn bulk_put(&self, kvs: impl Iterator<Item = TxIndexedData>) -> Result<()>;
}

#[enum_dispatch]
pub trait IndexStoreReader: Clone {
    async fn bulk_get(&self, keys: &[TxHash]) -> Result<HashMap<TxHash, TxIndexedData>>;
    async fn get(&self, key: &TxHash) -> Result<Option<TxIndexedData>>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
pub struct TxIndexedData {
    pub tx: TxEnvelope,
    pub trace: Vec<u8>,
    pub receipt: ReceiptEnvelope,
    pub header_subset: HeaderSubset,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, RlpEncodable, RlpDecodable,
)]
#[rlp(trailing)]
pub struct HeaderSubset {
    pub block_hash: BlockHash,
    pub block_number: u64,
    pub tx_index: u64,
    pub gas_used: u128,
    pub base_fee_per_gas: Option<u64>,
}

#[enum_dispatch]
pub trait BlobReader: Clone {
    async fn read(&self, key: &str) -> Result<Bytes>;
}

#[enum_dispatch(BlockDataReader)]
#[derive(Clone)]
pub enum BlockDataReaderErased {
    BlockDataArchive,
    TriedbReader,
}

#[enum_dispatch]
pub trait BlockDataReader: Clone {
    fn get_bucket(&self) -> &str;
    async fn get_latest(&self, latest_kind: LatestKind) -> Result<u64>;
    async fn get_block_by_number(&self, block_num: u64) -> Result<Block>;
    async fn get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Block>;
    async fn get_block_receipts(&self, block_number: u64) -> Result<Vec<ReceiptEnvelope>>;
    async fn get_block_traces(&self, block_number: u64) -> Result<Vec<Vec<u8>>>;
}

#[derive(Debug, Clone)]
pub enum BlockDataReaderArgs {
    Aws(AwsCliArgs),
    RocksDb(RocksDbCliArgs),
    Triedb(TrieDbCliArgs),
}

#[derive(Debug, Clone)]
pub enum ArchiveArgs {
    Aws(AwsCliArgs),
    RocksDb(RocksDbCliArgs),
}

impl FromStr for BlockDataReaderArgs {
    type Err = eyre::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use BlockDataReaderArgs::*;
        let mut words = s.split(' ');
        let Some(first) = words.next() else {
            bail!("Storage args string empty");
        };

        let next =
            |str: &'static str| -> Result<String> { Ok(words.next().ok_or_eyre(str)?.to_owned()) };

        Ok(match first.to_lowercase().as_str() {
            "aws" => Aws(AwsCliArgs::parse(next)?),
            "rocksdb" => RocksDb(RocksDbCliArgs::parse(next)?),
            "triedb" => Triedb(TrieDbCliArgs::parse(next)?),
            _ => {
                bail!("Unrecognized storage args variant: {first}");
            }
        })
    }
}

impl FromStr for ArchiveArgs {
    type Err = eyre::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use ArchiveArgs::*;
        let mut words = s.split(' ');
        let Some(first) = words.next() else {
            bail!("Storage args string empty");
        };

        let next =
            |str: &'static str| -> Result<String> { Ok(words.next().ok_or_eyre(str)?.to_owned()) };

        Ok(match first.to_lowercase().as_str() {
            "aws" => Aws(AwsCliArgs::parse(next)?),
            "rocksdb" => RocksDb(RocksDbCliArgs::parse(next)?),
            _ => {
                bail!("Unrecognized storage args variant: {first}");
            }
        })
    }
}

impl BlockDataReaderArgs {
    pub async fn build(&self, metrics: &Metrics) -> Result<BlockDataReaderErased> {
        use BlockDataReaderArgs::*;
        Ok(match self {
            Aws(args) => BlockDataArchive::new(args.build_blob_store(metrics).await).into(),
            RocksDb(args) => BlockDataArchive::new(RocksDbClient::try_from(args)?.into()).into(),
            Triedb(args) => TriedbReader::new(args).into(),
        })
    }
}

impl ArchiveArgs {
    pub async fn build_block_data_archive(&self, metrics: &Metrics) -> Result<BlockDataArchive> {
        let store: BlobStoreErased = match self {
            ArchiveArgs::Aws(args) => args.build_blob_store(metrics).await,
            ArchiveArgs::RocksDb(args) => RocksDbClient::try_from(args)?.into(),
        };
        Ok(BlockDataArchive::new(store))
    }

    pub async fn build_index_archive(&self, metrics: &Metrics) -> Result<TxIndexArchiver> {
        let (bstore, istore) = self.build_both(metrics).await?;
        Ok(TxIndexArchiver::new(istore, BlockDataArchive::new(bstore)))
    }

    async fn build_both(&self, metrics: &Metrics) -> Result<(BlobStoreErased, IndexStoreErased)> {
        Ok(match self {
            ArchiveArgs::Aws(args) => {
                let (b, s) = join!(
                    args.build_blob_store(metrics),
                    args.build_index_store(metrics)
                );
                (b, s.into())
            }
            ArchiveArgs::RocksDb(args) => {
                let store = args.build()?;
                (store.clone().into(), store.into())
            }
        })
    }

    pub async fn build_archive_reader(&self, metrics: &Metrics) -> Result<ArchiveReader> {
        let (b_reader, i_reader) = self.build_both(metrics).await?;
        Ok(ArchiveReader::new(
            BlockDataArchive::new(b_reader).into(),
            i_reader,
        ))
    }
}

#[derive(Clone, Debug)]
pub struct AwsCliArgs {
    pub bucket: String,
    pub concurrency: usize,
    pub region: Option<String>,
}

impl AwsCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<Self> {
        Ok(Self {
            bucket: next("args missing bucket")?,
            concurrency: usize::from_str(&next("args missing concurrency")?)?,
            region: next("").ok(),
        })
    }

    pub async fn build_blob_store(&self, metrics: &Metrics) -> BlobStoreErased {
        S3Bucket::new(
            self.bucket.clone(),
            &get_aws_config(self.region.clone()).await,
            metrics.clone(),
        )
        .into()
    }

    pub async fn build_index_store(&self, metrics: &Metrics) -> DynamoDBArchive {
        DynamoDBArchive::new(
            self.bucket.clone(),
            &get_aws_config(self.region.clone()).await,
            self.concurrency,
            metrics.clone(),
        )
    }
}

#[derive(Clone, Debug, Parser)]
pub struct RocksDbCliArgs {
    pub db_path: String,
}

impl RocksDbCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<Self> {
        Ok(Self {
            db_path: next("rocksdb args missing db path")?,
        })
    }

    pub fn build(&self) -> Result<RocksDbClient> {
        RocksDbClient::new(&self.db_path)
    }
}

#[derive(Clone, Debug)]
pub struct TrieDbCliArgs {
    pub triedb_path: String,
    pub max_concurrent_requests: usize,
}

impl TrieDbCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<TrieDbCliArgs> {
        Ok(TrieDbCliArgs {
            triedb_path: next("storage args missing db path")?,
            max_concurrent_requests: usize::from_str(&next(
                "args missing max_concurrent_requests",
            )?)?,
        })
    }
}

#[enum_dispatch(BlobReader, BlobStore)]
#[derive(Clone)]
pub enum BlobStoreErased {
    RocksDbClient,
    S3Bucket,
    MemoryStorage,
    FsStorage,
}

#[enum_dispatch(IndexStoreReader, IndexStore)]
#[derive(Clone)]
pub enum IndexStoreErased {
    RocksDbClient,
    DynamoDBArchive,
    MemoryStorage,
    FsStorage,
}
