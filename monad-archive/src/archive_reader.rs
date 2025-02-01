use std::ops::Deref;

use alloy_primitives::BlockHash;
use eyre::Result;
use monad_triedb_utils::triedb_env::ReceiptWithLogIndex;

use crate::{
    cli::AwsCliArgs,
    prelude::*,
    storage::{BlockDataWithOffsets, CloudProxyReader, KVReaderErased},
};

pub enum LatestKind {
    Uploaded,
    Indexed,
}

#[derive(Clone)]
pub struct ArchiveReader {
    block_data_reader: BlockDataReaderErased,
    index_reader: IndexReader,
}

impl ArchiveReader {
    pub fn new(
        block_data_reader: BlockDataReaderErased,
        index_store: KVReaderErased,
    ) -> ArchiveReader {
        ArchiveReader {
            index_reader: IndexReader::new(index_store, block_data_reader.clone()),
            block_data_reader,
        }
    }

    pub async fn initialize_reader(
        bucket: String,
        region: Option<String>,
        url: &str,
        api_key: &str,
        concurrency: usize,
    ) -> Result<ArchiveReader> {
        let url = url::Url::parse(url)?;
        let block_data_reader = BlockDataArchive::new(
            AwsCliArgs {
                bucket: bucket.clone(),
                concurrency,
                region,
            }
            .build_blob_store(&Metrics::none())
            .await,
        );
        let cloud_proxy_reader = CloudProxyReader::new(api_key, url, bucket)?;

        Ok(ArchiveReader::new(
            block_data_reader.into(),
            cloud_proxy_reader.into(),
        ))
    }
}

impl Deref for ArchiveReader {
    type Target = IndexReader;

    fn deref(&self) -> &Self::Target {
        &self.index_reader
    }
}

impl BlockDataReader for ArchiveReader {
    fn get_bucket(&self) -> &str {
        self.block_data_reader.get_bucket()
    }

    async fn get_latest(&self, latest_kind: LatestKind) -> Result<u64> {
        self.block_data_reader.get_latest(latest_kind).await
    }

    async fn get_block_by_number(&self, block_num: u64) -> Result<Block> {
        self.block_data_reader.get_block_by_number(block_num).await
    }

    async fn get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Block> {
        self.block_data_reader.get_block_by_hash(block_hash).await
    }

    async fn get_block_receipts(&self, block_number: u64) -> Result<Vec<ReceiptWithLogIndex>> {
        self.block_data_reader
            .get_block_receipts(block_number)
            .await
    }

    async fn get_block_traces(&self, block_number: u64) -> Result<Vec<Vec<u8>>> {
        self.block_data_reader.get_block_traces(block_number).await
    }

    async fn get_block_data_with_offsets(&self, block_num: u64) -> Result<BlockDataWithOffsets> {
        self.block_data_reader
            .get_block_data_with_offsets(block_num)
            .await
    }
}
