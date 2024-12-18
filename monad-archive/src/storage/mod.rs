pub mod dynamodb;
pub mod rocksdb_storage;
pub mod s3;
pub mod triedb_reader;

use std::collections::HashMap;
use std::str::FromStr;

use crate::{ArchiveReader, BlobStore, LatestKind, Metrics, TxIndexArchiver};
use crate::{IndexStore, IndexStoreReader, TxIndexedData};
use clap::{Parser, Subcommand};
use enum_dispatch::enum_dispatch;
use eyre::{bail, ContextCompat, OptionExt, Result};

pub use dynamodb::*;
use futures::FutureExt;
use reth_primitives::{Block, BlockHash, ReceiptWithBloom};
pub use rocksdb_storage::*;
pub use s3::*;
use tokio::{join, try_join};

use crate::archive_block_data::BlockDataArchive;

use crate::triedb_reader::TriedbReader;

#[enum_dispatch(BlockDataReader)]
#[derive(Clone)]
pub enum BlockDataReaderErased {
    BlockDataArchive,
    TriedbReader,
    // RpcBlockDataReader
}

#[enum_dispatch]
pub trait BlockDataReader: Clone {
    fn get_bucket(&self) -> &str;
    async fn get_latest(&self, latest_kind: LatestKind) -> Result<u64>;
    async fn get_block_by_number(&self, block_num: u64) -> Result<Block>;
    async fn get_block_by_hash(&self, block_hash: BlockHash) -> Result<Block>;
    async fn get_block_receipts(&self, block_number: u64) -> Result<Vec<ReceiptWithBloom>>;
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

// #[derive(Debug, Clone)]
// pub enum BlockDataArchiverArgs {
//     Aws(AwsCliArgs),
//     RocksDb(RocksDbCliArgs),
// }
//
// #[derive(Debug, Clone)]
// pub enum TxIndexArchiverArgs {
//     Aws(AwsCliArgs),
//     RocksDb(RocksDbCliArgs),
// }
//
// #[derive(Debug, Clone)]
// pub enum ArchiveReaderArgs {
//     Aws(AwsCliArgs),
//     RocksDb(RocksDbCliArgs),
// }

#[derive(Debug, Clone)]
pub enum StorageArgs {
    Aws(AwsCliArgs),
    RocksDb(RocksDbCliArgs),
    Triedb(TrieDbCliArgs),
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
//
// impl FromStr for TxIndexArchiverArgs {
//     type Err = eyre::Error;
//
//     fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
//         use TxIndexArchiverArgs::*;
//         let mut words = s.split(' ');
//         let Some(first) = words.next() else {
//             bail!("Storage args string empty");
//         };
//
//         let next =
//             |str: &'static str| -> Result<String> { Ok(words.next().ok_or_eyre(str)?.to_owned()) };
//
//         Ok(match first.to_lowercase().as_str() {
//             "aws" => Aws(AwsCliArgs::parse(next)?),
//             "rocksdb" => RocksDb(RocksDbCliArgs::parse(next)?),
//             _ => {
//                 bail!("Unrecognized storage args variant: {first}");
//             }
//         })
//     }
// }

impl FromStr for StorageArgs {
    type Err = eyre::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let mut words = s.split(' ');
        let Some(first) = words.next() else {
            bail!("Storage args string empty");
        };

        let next =
            |str: &'static str| -> Result<String> { Ok(words.next().ok_or_eyre(str)?.to_owned()) };

        Ok(match first.to_lowercase().as_str() {
            "aws" => StorageArgs::Aws(AwsCliArgs::parse(next)?),
            "rocksdb" => StorageArgs::RocksDb(RocksDbCliArgs::parse(next)?),
            "triedb" => StorageArgs::Triedb(TrieDbCliArgs::parse(next)?),
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

    pub async fn build_index_archive(
        &self,
        concurrency: usize,
        metrics: &Metrics,
    ) -> Result<TxIndexArchiver> {
        let (bstore, istore) = self.build_both(concurrency, metrics).await?;
        Ok(TxIndexArchiver::new(istore, BlockDataArchive::new(bstore)))
    }

    async fn build_both(
        &self,
        concurrency: usize,
        metrics: &Metrics,
    ) -> Result<(BlobStoreErased, IndexStoreErased)> {
        Ok(match self {
            ArchiveArgs::Aws(args) => {
                let (b, s) = join!(
                    args.build_blob_store(metrics),
                    args.build_index_store(concurrency, metrics)
                );
                (b, s.into())
            }
            ArchiveArgs::RocksDb(args) => {
                let store = args.build()?;
                (store.clone().into(), store.into())
            }
        })
    }

    pub async fn build_archive_reader(
        &self,
        concurrency: usize,
        metrics: &Metrics,
    ) -> Result<ArchiveReader> {
        let (b_reader, i_reader) = self.build_both(concurrency, metrics).await?;
        Ok(ArchiveReader::new(
            BlockDataArchive::new(b_reader).into(),
            i_reader,
        ))
    }
}

// impl TxIndexArchiverArgs {
//     pub async fn build(&self, concurrency: usize, metrics: &Metrics) -> Result<TxIndexArchiver> {
//         let store: IndexStoreErased = match self {
//             TxIndexArchiverArgs::Aws(args) => {
//                 args.build_index_store(concurrency, metrics).await.into()
//             }
//             TxIndexArchiverArgs::RocksDb(args) => args.build()?.into(),
//         };
//         Ok(TxIndexArchiver::new(store))
//     }
// }
//
// impl ArchiveReaderArgs {
//     pub async fn build(&self, concurrency: usize, metrics: &Metrics) -> Result<ArchiveReader> {
//         let (b_reader, i_reader): (BlobStoreErased, IndexStoreErased) = match self {
//             ArchiveReaderArgs::Aws(args) => {
//                 let (b, s) = join!(
//                     args.build_blob_store(metrics),
//                     args.build_index_store(concurrency, metrics)
//                 );
//                 (b, s.into())
//             }
//             ArchiveReaderArgs::RocksDb(args) => {
//                 let store = args.build()?;
//                 (store.clone().into(), store.into())
//             }
//         };
//         Ok(ArchiveReader::new(
//             BlockDataArchive::new(b_reader).into(),
//             i_reader,
//         ))
//     }
// }

impl StorageArgs {
    pub async fn build_block_data_reader(&self, metrics: Metrics) -> Result<BlockDataReaderErased> {
        Ok(match self {
            StorageArgs::Aws(aws_cli_args) => BlockDataArchive::new(
                S3Bucket::new(
                    aws_cli_args.bucket.clone(),
                    &get_aws_config(aws_cli_args.region.clone()).await,
                    metrics.clone(),
                )
                .into(),
            )
            .into(),
            StorageArgs::RocksDb(args) => {
                BlockDataArchive::new(RocksDbClient::try_from(args)?.into()).into()
            }
            StorageArgs::Triedb(args) => TriedbReader::new(args).into(),
        })
    }

    pub async fn build_stores(
        &self,
        concurrency: usize,
        metrics: Metrics,
    ) -> Result<(BlobStoreErased, IndexStoreErased)> {
        Ok(match self {
            StorageArgs::Aws(aws_cli_args) => {
                let config = get_aws_config(aws_cli_args.region.clone()).await;
                (
                    S3Bucket::new(aws_cli_args.bucket.clone(), &config, metrics.clone()).into(),
                    DynamoDBArchive::new(
                        aws_cli_args.bucket.clone(),
                        &config,
                        concurrency,
                        metrics,
                    )
                    .into(),
                )
            }
            StorageArgs::RocksDb(rocks_db_cli_args) => {
                let db = RocksDbClient::new(&rocks_db_cli_args.db_path)?;
                (db.clone().into(), db.into())
            }
            StorageArgs::Triedb(trie_db_cli_args) => todo!(),
        })
    }
}

#[derive(Clone, Debug, Parser)]
pub struct AwsCliArgs {
    #[arg(long)]
    pub region: Option<String>,

    #[arg(long)]
    pub bucket: String,
}

impl AwsCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<Self> {
        Ok(Self {
            bucket: next("args missing bucket")?,
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

    pub async fn build_index_store(
        &self,
        concurrency: usize,
        metrics: &Metrics,
    ) -> DynamoDBArchive {
        DynamoDBArchive::new(
            self.bucket.clone(),
            &get_aws_config(self.region.clone()).await,
            concurrency,
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
            db_path: next("storage args missing db path")?,
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

use crate::archive_block_data::BlobReader;
use bytes::Bytes;

#[enum_dispatch(BlobReader, BlobStore)]
#[derive(Clone)]
pub enum BlobStoreErased {
    RocksDbClient,
    S3Bucket,
}

#[enum_dispatch(IndexStoreReader, IndexStore)]
#[derive(Clone)]
pub enum IndexStoreErased {
    RocksDbClient,
    DynamoDBArchive,
}
