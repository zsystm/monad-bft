use eyre::Result;
use reth_primitives::{ReceiptWithBloom, TransactionSigned};

use crate::triedb::BlockHeader;

pub enum LatestKind {
    Uploaded,
    Indexed,
}

pub trait ArchiveWriterInterface {
    // Get the latest stored block
    async fn get_latest(&self, latest_kind: LatestKind) -> Result<u64>;

    // Update latest processed block
    async fn update_latest(&self, block_num: u64, latest_kind: LatestKind) -> Result<()>;

    /*
        Archive Block
        1) Table1: key = block_number, value = RLP(block)
        2) Table2: key = block_hash, value = block_number
    */
    async fn archive_block(
        &self,
        block_header: BlockHeader,
        transactions: Vec<TransactionSigned>,
        block_num: u64,
    ) -> Result<()>;

    /*
        Archive Receipt
        Table: key = block_number, value = RLP(Vec<Receipt>)
    */
    async fn archive_receipts(&self, receipts: Vec<ReceiptWithBloom>, block_num: u64)
        -> Result<()>;

    async fn archive_traces(&self, traces: Vec<Vec<u8>>, block_num: u64) -> Result<()>;
}
