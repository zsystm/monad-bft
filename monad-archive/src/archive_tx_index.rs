use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{dynamodb::DynamoDBArchive, IndexStoreErased};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use aws_config::SdkConfig;
use aws_sdk_dynamodb::{
    types::{AttributeValue, KeysAndAttributes, PutRequest, WriteRequest},
    Client,
};
use eyre::{bail, Context, Result};
use futures::future::join_all;
use reth_primitives::{Block, BlockHash, ReceiptWithBloom, TransactionSigned, U128, U256};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use tracing::error;

pub trait IndexStore: Clone {
    async fn bulk_put(&self, kvs: impl Iterator<Item = TxIndexedData>) -> Result<()>;
    async fn bulk_get(&self, keys: &[String]) -> Result<HashMap<String, TxIndexedData>>;
    async fn get(&self, key: impl Into<String>) -> Result<Option<TxIndexedData>>;
}

#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, RlpEncodable, RlpDecodable,
)]
pub struct TxIndexedData {
    pub tx: TransactionSigned,
    pub trace: Vec<u8>,
    pub receipt: ReceiptWithBloom,
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
    pub gas_used: u64,
    pub base_fee_per_gas: Option<u64>,
}

pub struct TxIndexArchiver<Store = IndexStoreErased> {
    pub store: Arc<Store>,
}

impl<T> Clone for TxIndexArchiver<T> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
        }
    }
}

impl<Store: IndexStore> TxIndexArchiver<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            store: Arc::new(store),
        }
    }

    pub async fn index_block(
        &self,
        block: Block,
        traces: Vec<Vec<u8>>,
        receipts: Vec<ReceiptWithBloom>,
    ) -> Result<()> {
        let block_number = block.number;
        let block_hash = block.hash_slow();
        let base_fee_per_gas = block.base_fee_per_gas;

        let mut prev_cumulative_gas_used = 0;

        let requests = block
            .body
            .into_iter()
            .zip(traces.into_iter())
            .zip(receipts.into_iter())
            .enumerate()
            .map(|(idx, ((tx, trace), receipt))| {
                // calculate gas used by this tx
                let gas_used = receipt.receipt.cumulative_gas_used - prev_cumulative_gas_used;
                prev_cumulative_gas_used = receipt.receipt.cumulative_gas_used;

                TxIndexedData {
                    tx,
                    trace,
                    receipt,
                    header_subset: HeaderSubset {
                        block_hash,
                        block_number,
                        tx_index: idx as u64,
                        gas_used: gas_used,
                        base_fee_per_gas,
                    },
                }
            });

        self.store.bulk_put(requests).await
    }
}
