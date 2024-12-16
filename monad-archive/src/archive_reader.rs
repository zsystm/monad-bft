use std::collections::HashMap;

use eyre::Result;
use reth_primitives::{Block, ReceiptWithBloom};

use crate::*;

pub enum LatestKind {
    Uploaded,
    Indexed,
}

#[derive(Clone)]
pub struct ArchiveReader<BStore = BlobStoreErased, IStore = IndexStoreErased> {
    block_store_archive: BlockDataArchive<BStore>,
    index_store: IStore,
}

impl ArchiveReader {
    pub async fn new(storage_args: &StorageArgs, metrics: &Metrics) -> Result<ArchiveReader> {
        let (blob_store, index_store) = storage_args.build_stores(metrics.clone()).await?;
        Ok(ArchiveReader {
            block_store_archive: BlockDataArchive::new(blob_store),
            index_store,
        })
    }
}

impl<BStore: BlobStore, IStore: IndexStore> ArchiveReader<BStore, IStore> {
    pub fn bucket(&self) -> &str {
        &self.block_store_archive.bucket.bucket_name()
    }

    pub async fn batch_get_txdata(
        &self,
        keys: &[String],
    ) -> Result<HashMap<String, TxIndexedData>> {
        self.index_store.bulk_get(keys).await
    }
    pub async fn get_txdata(&self, key: impl Into<String>) -> Result<Option<TxIndexedData>> {
        self.index_store.get(key).await
    }

    // Get the latest stored block
    pub async fn get_latest(&self, latest_kind: LatestKind) -> Result<u64> {
        self.block_store_archive.get_latest(latest_kind).await
    }

    pub async fn get_block_by_hash(&self, block_hash: &[u8; 32]) -> Result<Block> {
        self.block_store_archive.get_block_by_hash(block_hash).await
    }

    pub async fn get_block_by_number(&self, block_num: u64) -> Result<Block> {
        self.block_store_archive
            .get_block_by_number(block_num)
            .await
    }

    pub async fn get_block_receipts(&self, block_number: u64) -> Result<Vec<ReceiptWithBloom>> {
        self.block_store_archive
            .get_block_receipts(block_number)
            .await
    }

    pub async fn get_block_traces(&self, block_number: u64) -> Result<Vec<Vec<u8>>> {
        self.block_store_archive
            .get_block_traces(block_number)
            .await
    }
}
