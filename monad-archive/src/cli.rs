use std::str::FromStr;

use aws_config::{
    meta::region::RegionProviderChain, retry::RetryConfig, timeout::TimeoutConfig, BehaviorVersion,
    Region, SdkConfig,
};
use eyre::{bail, OptionExt};
use futures::join;
use serde::{Deserialize, Serialize};

use crate::{
    kvstore::{
        mongo::{self, MongoDbStorage},
        object_store::ObjectStore,
        rocksdb_storage::RocksDbClient,
    },
    model::BlockArchiverErased,
    model_v2::ModelV2,
    prelude::*,
};

const DEFAULT_BLOB_STORE_TIMEOUT: u64 = 30;
const DEFAULT_INDEX_STORE_TIMEOUT: u64 = 20;

pub async fn get_aws_config(region: Option<String>, timeout_secs: u64) -> SdkConfig {
    let region_provider = RegionProviderChain::default_provider().or_else(
        region
            .map(Region::new)
            .unwrap_or_else(|| Region::new("us-east-2")),
    );

    info!(
        "Running in region: {}",
        region_provider
            .region()
            .await
            .map(|r| r.to_string())
            .unwrap_or("No region found".into())
    );

    aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .timeout_config(
            TimeoutConfig::builder()
                .operation_timeout(Duration::from_secs(timeout_secs))
                .operation_attempt_timeout(Duration::from_secs(timeout_secs))
                .read_timeout(Duration::from_secs(timeout_secs))
                .build(),
        )
        .retry_config(RetryConfig::adaptive())
        .load()
        .await
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum BlockDataReaderArgs {
    Aws(AwsCliArgs),
    RocksDb(RocksDbCliArgs),
    Triedb(TrieDbCliArgs),
    MongoDb(MongoDbCliArgs),
    V2(V2CliArgs),
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum ArchiveArgs {
    Aws(AwsCliArgs),
    RocksDb(RocksDbCliArgs),
    MongoDb(MongoDbCliArgs),
    V2(V2CliArgs),
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
            "mongodb" => MongoDb(MongoDbCliArgs::parse(next)?),
            "v2" => V2(V2CliArgs::parse(next)?),
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
            "mongodb" => MongoDb(MongoDbCliArgs::parse(next)?),
            "v2" => V2(V2CliArgs::parse(next)?),
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
            RocksDb(args) => BlockDataArchive::new(RocksDbClient::try_from(args)?).into(),
            Triedb(args) => TriedbReader::new(args).into(),
            MongoDb(args) => BlockDataArchive::new(
                MongoDbStorage::new_block_store(&args.url, &args.db, None).await?,
            )
            .into(),
            V2(v2_cli_args) => v2_cli_args.build_model_v2(metrics).await?.into(),
        })
    }

    pub fn replica_name(&self) -> String {
        use BlockDataReaderArgs::*;
        match self {
            Aws(aws_cli_args) => aws_cli_args.bucket.clone(),
            RocksDb(rocks_db_cli_args) => rocks_db_cli_args.db_path.clone(),
            Triedb(trie_db_cli_args) => trie_db_cli_args.triedb_path.clone(),
            MongoDb(mongo_db_cli_args) => {
                format!("{}:{}", mongo_db_cli_args.url, mongo_db_cli_args.db)
            }
            V2(v2_cli_args) => v2_cli_args.replica_name.clone(),
        }
    }
}

impl ArchiveArgs {
    pub async fn build_block_archiver(&self, metrics: &Metrics) -> Result<BlockArchiverErased> {
        Ok(match self {
            ArchiveArgs::Aws(args) => {
                let store = args.build_blob_store(metrics).await;
                BlockArchiverErased::BlockDataArchive(BlockDataArchive::new(store))
            }
            ArchiveArgs::RocksDb(args) => {
                let store = KVStoreErased::from(args.build()?);
                BlockArchiverErased::BlockDataArchive(BlockDataArchive::new(store))
            }
            ArchiveArgs::MongoDb(args) => {
                let store =
                    MongoDbStorage::new_block_store(&args.url, &args.db, args.capped_size_gb)
                        .await?;
                BlockArchiverErased::BlockDataArchive(BlockDataArchive::new(store))
            }
            ArchiveArgs::V2(args) => args.build_model_v2(metrics).await?.into(),
        })
    }

    pub async fn build_index_archive(
        &self,
        metrics: &Metrics,
        max_inline_encoded_len: usize,
    ) -> Result<TxIndexArchiver> {
        let (blob, index) = match self {
            ArchiveArgs::Aws(args) => {
                join!(
                    args.build_blob_store(metrics),
                    args.build_index_store(metrics)
                )
            }
            ArchiveArgs::RocksDb(args) => {
                let store = KVStoreErased::from(args.build()?);
                (store.clone(), store)
            }
            ArchiveArgs::MongoDb(args) => (
                MongoDbStorage::new_block_store(&args.url, &args.db, None)
                    .await?
                    .into(),
                MongoDbStorage::new_index_store(&args.url, &args.db, args.capped_size_gb)
                    .await?
                    .into(),
            ),
            ArchiveArgs::V2(_) => {
                unimplemented!("V2 index archive does not exist, indexing done at time of writing")
            }
        };
        Ok(TxIndexArchiver::new(
            index,
            BlockDataArchive::new(blob),
            max_inline_encoded_len,
        ))
    }

    pub async fn build_archive_reader(&self, metrics: &Metrics) -> Result<ArchiveReader> {
        match self {
            ArchiveArgs::Aws(args) => {
                let (blob, index) = join!(
                    args.build_blob_store(metrics),
                    args.build_index_store(metrics)
                );
                let bdr = BlockDataArchive::new(blob);
                Ok(ArchiveReader::new(
                    bdr.clone(),
                    IndexReaderImpl::new(index, bdr),
                    None,
                    None,
                ))
            }
            ArchiveArgs::RocksDb(args) => {
                let store = KVStoreErased::from(args.build()?);
                let bdr = BlockDataArchive::new(store.clone());
                Ok(ArchiveReader::new(
                    bdr.clone(),
                    IndexReaderImpl::new(store, bdr),
                    None,
                    None,
                ))
            }
            ArchiveArgs::MongoDb(args) => {
                let (blob, index) = try_join!(
                    MongoDbStorage::new_block_store(&args.url, &args.db, None),
                    MongoDbStorage::new_index_store(&args.url, &args.db, args.capped_size_gb),
                )?;
                let bdr = BlockDataArchive::new(blob);
                Ok(ArchiveReader::new(
                    bdr.clone(),
                    IndexReaderImpl::new(index, bdr),
                    None,
                    None,
                ))
            }
            ArchiveArgs::V2(v2_cli_args) => {
                let model_v2 = v2_cli_args.build_model_v2(metrics).await?;
                Ok(ArchiveReader::new(model_v2.clone(), model_v2, None, None))
            }
        }
    }

    pub fn replica_name(&self) -> String {
        match self {
            ArchiveArgs::Aws(aws_cli_args) => aws_cli_args.bucket.clone(),
            ArchiveArgs::RocksDb(rocks_db_cli_args) => rocks_db_cli_args.db_path.clone(),
            ArchiveArgs::MongoDb(mongo_db_cli_args) => mongo_db_cli_args.db.clone(),
            ArchiveArgs::V2(v2_cli_args) => v2_cli_args.replica_name.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct AwsCliArgs {
    pub bucket: String,
    pub concurrency: usize,
    pub region: Option<String>,
}

impl AwsCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<Self> {
        Ok(Self {
            bucket: next("args missing bucket")?,
            concurrency: usize::from_str(&next("").unwrap_or_else(|_| "50".to_string()))?,
            region: next("").ok(),
        })
    }

    pub async fn build_blob_store(&self, metrics: &Metrics) -> KVStoreErased {
        S3Bucket::new(
            self.bucket.clone(),
            &get_aws_config(self.region.clone(), DEFAULT_BLOB_STORE_TIMEOUT).await,
            metrics.clone(),
        )
        .into()
    }

    pub async fn build_index_store(&self, metrics: &Metrics) -> KVStoreErased {
        let config = &get_aws_config(self.region.clone(), DEFAULT_INDEX_STORE_TIMEOUT).await;

        DynamoDBArchive::new(
            S3Bucket::new(self.bucket.clone(), config, metrics.clone()),
            self.bucket.clone(),
            config,
            self.concurrency,
            metrics.clone(),
        )
        .into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct TrieDbCliArgs {
    pub triedb_path: String,
    pub max_buffered_read_requests: usize,
    pub max_triedb_async_read_concurrency: usize,
    pub max_buffered_traverse_requests: usize,
    pub max_triedb_async_traverse_concurrency: usize,
    pub max_finalized_block_cache_len: usize,
    pub max_voted_block_cache_len: usize,
}

impl TrieDbCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<TrieDbCliArgs> {
        Ok(TrieDbCliArgs {
            triedb_path: next("storage args missing db path")?,
            max_buffered_read_requests: usize::from_str(&next(
                "args missing max_buffered_read_requests",
            )?)?,
            max_triedb_async_read_concurrency: 10000,
            max_buffered_traverse_requests: 200,
            max_triedb_async_traverse_concurrency: 20,
            max_finalized_block_cache_len: 200,
            max_voted_block_cache_len: 3,
        })
    }
}

#[derive(Debug, Clone, Serialize, Hash, PartialEq, Eq)]
pub struct V2CliArgs {
    pub url: String,
    pub replica_name: String,
    pub concurrency: Option<usize>,
    pub region: Option<String>,
}

impl<'de> Deserialize<'de> for V2CliArgs {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        let mut words = s.split(' ');
        Ok(V2CliArgs {
            url: words
                .next()
                .ok_or_else(|| serde::de::Error::custom("storage args missing url"))?
                .to_string(),
            replica_name: words
                .next()
                .ok_or_else(|| serde::de::Error::custom("storage args missing replica name"))?
                .to_string(),
            concurrency: words.next().and_then(|s| usize::from_str(s).ok()),
            region: words.next().map(|s| s.to_string()),
        })
    }
}

impl V2CliArgs {
    pub fn try_from_str(s: &str) -> Result<Self> {
        let mut words = s.split(' ');
        let next =
            |str: &'static str| -> Result<String> { Ok(words.next().ok_or_eyre(str)?.to_owned()) };
        Self::parse(next)
    }

    fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<Self> {
        Ok(Self {
            url: next("storage args missing url")?,
            replica_name: next("storage args missing replica name")?,
            concurrency: next("").ok().and_then(|s| usize::from_str(&s).ok()),
            region: next("").ok(),
        })
    }

    pub(crate) async fn build_model_v2(&self, metrics: &Metrics) -> Result<ModelV2> {
        let mongo_client = mongo::new_client(&self.url).await?;
        let s3_client = S3Bucket::new(
            self.replica_name.clone(),
            &get_aws_config(self.region.clone(), DEFAULT_BLOB_STORE_TIMEOUT).await,
            metrics.clone(),
        );

        ModelV2::from_client(mongo_client, ObjectStore::S3(s3_client), 20, self.replica_name.clone()).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct MongoDbCliArgs {
    pub url: String,
    pub db: String,
    pub capped_size_gb: Option<u64>,
}

impl MongoDbCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<Self> {
        Ok(MongoDbCliArgs {
            url: next("storage args missing mongo url")?,
            db: next("storage args missing mongo db name")?,
            capped_size_gb: next("").ok().and_then(|s| u64::from_str(&s).ok()),
        })
    }
}
