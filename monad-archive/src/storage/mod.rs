pub mod dynamodb;
pub mod rocksdb_storage;
pub mod s3;

use crate::{BlobStore, IndexStore, Metrics};
use clap::{Parser, Subcommand};
use eyre::Result;

pub use dynamodb::*;
pub use rocksdb_storage::*;
pub use s3::*;

#[derive(Debug, Subcommand)]
pub enum StorageArgs {
    Aws(AwsCliArgs),
    RocksDb(RocksDbCliArgs),
}

impl StorageArgs {
    pub fn concurrency(&self) -> usize {
        match self {
            StorageArgs::Aws(aws_cli_args) => aws_cli_args.concurrency,
            StorageArgs::RocksDb(_rocks_db_cli_args) => 1,
        }
    }

    pub async fn build_stores(
        &self,
        metrics: Metrics,
    ) -> Result<(BlobStoreErased, IndexStoreErased)> {
        Ok(match self {
            StorageArgs::Aws(aws_cli_args) => {
                let config = get_aws_config(aws_cli_args.region.clone()).await;
                (
                    BlobStoreErased::S3(S3Bucket::new(
                        aws_cli_args.bucket.clone(),
                        &config,
                        metrics.clone(),
                    )),
                    IndexStoreErased::DynamoDb(DynamoDBArchive::new(
                        aws_cli_args.bucket.clone(),
                        &config,
                        aws_cli_args.concurrency,
                        metrics,
                    )),
                )
            }
            StorageArgs::RocksDb(rocks_db_cli_args) => {
                let db = RocksDbClient::new(&rocks_db_cli_args.db_path)?;
                (
                    BlobStoreErased::RocksDb(db.clone()),
                    IndexStoreErased::RocksDb(db),
                )
            }
        })
    }
}

#[derive(Debug, Parser)]
pub struct AwsCliArgs {
    #[arg(long)]
    pub region: Option<String>,

    #[arg(long)]
    pub bucket: String,

    #[arg(long, default_value_t = 5)]
    pub concurrency: usize,
}

#[derive(Debug, Parser)]
pub struct RocksDbCliArgs {
    pub db_path: String,
}

#[derive(Clone)]
pub enum BlobStoreErased {
    RocksDb(RocksDbClient),
    S3(S3Bucket),
}

impl BlobStore for BlobStoreErased {
    async fn upload(&self, key: &str, data: Vec<u8>) -> eyre::Result<()> {
        match self {
            BlobStoreErased::RocksDb(rocks_db_client) => rocks_db_client.upload(key, data).await,
            BlobStoreErased::S3(s3_bucket) => s3_bucket.upload(key, data).await,
        }
    }

    async fn read(&self, key: &str) -> eyre::Result<bytes::Bytes> {
        match self {
            BlobStoreErased::RocksDb(rocks_db_client) => rocks_db_client.read(key).await,
            BlobStoreErased::S3(s3_bucket) => s3_bucket.read(key).await,
        }
    }

    fn bucket_name(&self) -> &str {
        match self {
            BlobStoreErased::RocksDb(x) => x.bucket_name(),
            BlobStoreErased::S3(x) => x.bucket_name(),
        }
    }
}

#[derive(Clone)]
pub enum IndexStoreErased {
    RocksDb(RocksDbClient),
    DynamoDb(DynamoDBArchive),
}

impl IndexStore for IndexStoreErased {
    async fn bulk_put(&self, kvs: impl Iterator<Item = crate::TxIndexedData>) -> eyre::Result<()> {
        match self {
            IndexStoreErased::RocksDb(rocks_db_client) => rocks_db_client.bulk_put(kvs).await,
            IndexStoreErased::DynamoDb(dynamo_dbarchive) => dynamo_dbarchive.bulk_put(kvs).await,
        }
    }

    async fn bulk_get(
        &self,
        keys: &[String],
    ) -> eyre::Result<std::collections::HashMap<String, crate::TxIndexedData>> {
        match self {
            IndexStoreErased::RocksDb(rocks_db_client) => rocks_db_client.bulk_get(keys).await,
            IndexStoreErased::DynamoDb(dynamo_dbarchive) => dynamo_dbarchive.bulk_get(keys).await,
        }
    }

    async fn get(&self, key: impl Into<String>) -> eyre::Result<Option<crate::TxIndexedData>> {
        match self {
            IndexStoreErased::RocksDb(rocks_db_client) => rocks_db_client.get(key).await,
            IndexStoreErased::DynamoDb(dynamo_dbarchive) => dynamo_dbarchive.get(key).await,
        }
    }
}
