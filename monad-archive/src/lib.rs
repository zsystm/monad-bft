use reth_primitives::{Block, TxHash};

// pub mod archive_interface;
pub mod archive_writer;
pub mod cli_base;
pub mod errors;
pub mod kv_interface;
pub mod triedb;

trait TxHashIndexUplaoder {
    // fn upload_tx_index(&self, txhash: TxHash, block: Block)
}
