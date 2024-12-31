use std::collections::HashMap;

use alloy_consensus::{ReceiptEnvelope, ReceiptWithBloom};
use alloy_primitives::BlockHash;
use eyre::Result;

use crate::*;

pub enum LatestKind {
    Uploaded,
    Indexed,
}

#[derive(Clone)]
pub struct ArchiveReader<
    BReader: BlockDataReader = BlockDataReaderErased,
    IReader: IndexStoreReader = IndexStoreErased,
> {
    block_data_reader: BReader,
    index_reader: IReader,
}

impl<BStore: BlockDataReader, IStore: IndexStoreReader> ArchiveReader<BStore, IStore> {
    pub fn new(block_data_reader: BStore, index_reader: IStore) -> ArchiveReader<BStore, IStore> {
        ArchiveReader {
            block_data_reader,
            index_reader,
        }
    }
}

impl<BStore: BlockDataReader, IStore: IndexStoreReader> IndexStoreReader
    for ArchiveReader<BStore, IStore>
{
    async fn bulk_get(&self, keys: &[String]) -> Result<HashMap<String, TxIndexedData>> {
        self.index_reader.bulk_get(keys).await
    }

    async fn get(&self, key: impl Into<String>) -> Result<Option<TxIndexedData>> {
        self.index_reader.get(key).await
    }
}

impl<BStore: BlockDataReader, IStore: IndexStoreReader> BlockDataReader
    for ArchiveReader<BStore, IStore>
{
    fn get_bucket(&self) -> &str {
        self.block_data_reader.get_bucket()
    }

    async fn get_latest(&self, latest_kind: LatestKind) -> Result<u64> {
        self.block_data_reader.get_latest(latest_kind).await
    }

    async fn get_block_by_number(&self, block_num: u64) -> Result<Block> {
        self.block_data_reader.get_block_by_number(block_num).await
    }

    async fn get_block_by_hash(&self, block_hash: BlockHash) -> Result<Block> {
        self.block_data_reader.get_block_by_hash(block_hash).await
    }

    async fn get_block_receipts(&self, block_number: u64) -> Result<Vec<ReceiptEnvelope>> {
        self.block_data_reader
            .get_block_receipts(block_number)
            .await
    }

    async fn get_block_traces(&self, block_number: u64) -> Result<Vec<Vec<u8>>> {
        self.block_data_reader.get_block_traces(block_number).await
    }
}
