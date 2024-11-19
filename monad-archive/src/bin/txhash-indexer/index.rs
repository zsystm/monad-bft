use std::str::FromStr;

use eyre::Context;
use monad_archive::kv_interface::{KVStore, Store};
use reth_primitives::TxHash;

use crate::Result;

pub struct TxIndex {
    kv: Store,
}

impl TxIndex {
    const TX_PREFIX: &str = "tx";

    pub fn new(kv: Store) -> TxIndex {
        TxIndex { kv }
    }

    fn tx_key(txhash: TxHash) -> String {
        let hex_hash = format!("{:x}", txhash);
        format!("{}/{}/{}", Self::TX_PREFIX, &hex_hash[0..2], &hex_hash[2..])
    }

    pub async fn upload(&self, txhash: TxHash, block_num: u64) -> Result {
        let data = block_num.to_string().into();
        self.kv.upload(&Self::tx_key(txhash), data).await
    }

    pub async fn upload_batch(
        &self,
        tx_hashes: impl Iterator<Item = TxHash>,
        block_num: u64,
    ) -> Result {
        let data = block_num.to_string().into();
        let keys = tx_hashes.map(Self::tx_key);
        self.kv.upload_batch(keys, data).await
    }

    pub async fn blocknum_for_tx(&self, txhash: TxHash) -> Result<u64> {
        let bytes: &[u8] = &self.kv.read(&Self::tx_key(txhash)).await?;
        Ok(u64::from_le_bytes(bytes.try_into()?))
    }
}
