use core::str;
use std::sync::Arc;

use alloy_rlp::{Encodable, RlpDecodable, RlpEncodable};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{
    config::{BehaviorVersion, Region},
    primitives::ByteStream,
    Client,
};
use bytes::Bytes;
use eyre::{bail, Context};
use futures::{future::join_all, try_join};
use reth_primitives::{Block, ReceiptWithBloom, TransactionSigned};
use tokio::{sync::Semaphore, time::Duration};
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use tracing::{error, warn};

use crate::{errors::ArchiveError, kv_interface::KVStore, triedb::BlockHeader};

const BLOCK_PADDING_WIDTH: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq, Default, RlpEncodable, RlpDecodable)]
pub struct TxHashTable {
    pub tx: TransactionSigned,
    pub receipt: ReceiptWithBloom,
    pub trace: Vec<u8>,
}

impl TxHashTable {
    pub fn new(tx: TransactionSigned, receipt: ReceiptWithBloom, trace: Vec<u8>) -> Self {
        TxHashTable { tx, receipt, trace }
    }
}

#[derive(Clone)]
pub struct ArchiveWriter<KV: KVStore> {
    kv: KV,
    pub latest_indexed_table_key: &'static str,
    pub latest_uploaded_table_key: &'static str,

    // key =  {block}/{block_number}, value = {RLP(Block)}
    pub block_table_prefix: &'static str,

    // key = {block_hash}/{$block_hash}, value = {str(block_number)}
    pub block_hash_table_prefix: &'static str,

    // key = {receipts}/{block_number}, value = {RLP(Vec<Receipt>)}
    pub receipts_table_prefix: &'static str,

    // key = {traces}/{block_number}, value = {RLP(Vec<Vec<u8>>)}
    pub traces_table_prefix: &'static str,

    // key = {hash}/{tx_hash}, value = {RLP(TransactionSigned, ReceiptWithBloom, Vec<u8>)}
    pub hash_table_prefix: &'static str,
}

enum LatestSuffix {
    Uploaded,
    Indexed,
}

impl<KV: KVStore> ArchiveWriter<KV> {
    pub fn new(kv: KV) -> Self {
        Self {
            kv,
            block_table_prefix: "block",
            block_hash_table_prefix: "block_hash",
            receipts_table_prefix: "receipts",
            traces_table_prefix: "traces",
            hash_table_prefix: "hash",
            latest_indexed_table_key: "latest_indexed",
            latest_uploaded_table_key: "latest_uploaded",
        }
    }

    pub async fn read_block(&self, block_num: u64) -> Result<Block, ArchiveError> {
        todo!()
    }

    async fn get_latest(&self, lastest_suffix: LatestSuffix) -> Result<u64, ArchiveError> {
        let key = match lastest_suffix {
            LatestSuffix::Uploaded => &self.latest_uploaded_table_key,
            LatestSuffix::Indexed => &self.latest_indexed_table_key,
        };

        let value = self.kv.read(key).await?;

        let value_str = String::from_utf8(value.to_vec()).wrap_err("Invalid UTF-8 sequence")?;

        // Parse the string as u64
        value_str.parse::<u64>().wrap_err(format!(
            "Unable to convert block_number string to number (u64), value: {value_str}"
        ))
    }

    async fn update_latest(
        &self,
        block_num: u64,
        lastest_suffix: LatestSuffix,
    ) -> Result<(), ArchiveError> {
        let key = match lastest_suffix {
            LatestSuffix::Uploaded => &self.latest_uploaded_table_key,
            LatestSuffix::Indexed => &self.latest_indexed_table_key,
        };
        let latest_value = format!("{:0width$}", block_num, width = BLOCK_PADDING_WIDTH);
        self.kv.upload(&key, latest_value.as_bytes().to_vec()).await
    }
}

impl<KV: KVStore + Send + Sync + 'static> ArchiveWriter<KV> {
    pub async fn get_latest_uploaded(&self) -> Result<u64, ArchiveError> {
        self.get_latest(LatestSuffix::Uploaded).await
    }

    pub async fn get_latest_indexed(&self) -> Result<u64, ArchiveError> {
        self.get_latest(LatestSuffix::Indexed).await
    }

    pub async fn update_latest_uploaded(&self, block_num: u64) -> Result<(), ArchiveError> {
        self.update_latest(block_num, LatestSuffix::Uploaded).await
    }

    pub async fn update_latest_indexed(&self, block_num: u64) -> Result<(), ArchiveError> {
        self.update_latest(block_num, LatestSuffix::Indexed).await
    }

    pub async fn archive_block(
        &self,
        block_header: BlockHeader,
        transactions: Vec<TransactionSigned>,
        block_num: u64,
    ) -> Result<(), ArchiveError> {
        // 1) Insert into block table
        let block_key = format!(
            "{}/{:0width$}",
            self.block_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        );

        let block = make_block(block_header.clone(), transactions.clone());
        let mut rlp_block = Vec::with_capacity(8096);
        block.encode(&mut rlp_block);

        // 2) Insert into block_hash table
        let block_hash_key_suffix = hex::encode(block_header.hash);
        let block_hash_key = format!("{}/{}", self.block_hash_table_prefix, block_hash_key_suffix);

        // 3) Join futures
        try_join!(
            self.kv.upload(&block_key, rlp_block),
            self.kv
                .upload(&block_hash_key, block_num.to_string().as_bytes().to_vec())
        )?;

        Ok(())
    }

    pub async fn archive_receipts(
        &self,
        receipts: Vec<ReceiptWithBloom>,
        block_num: u64,
    ) -> Result<(), ArchiveError> {
        // 1) Prepare the receipts upload
        let receipts_key = format!(
            "{}/{:0width$}",
            self.receipts_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        );

        let mut rlp_receipts = Vec::new();
        receipts.encode(&mut rlp_receipts);
        self.kv.upload(&receipts_key, rlp_receipts).await
    }

    pub async fn archive_traces(
        &self,
        traces: Vec<Vec<u8>>,
        block_num: u64,
    ) -> Result<(), ArchiveError> {
        let traces_key = format!(
            "{}/{:0width$}",
            self.traces_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        );

        let mut rlp_traces = vec![];
        traces.encode(&mut rlp_traces);

        self.kv.upload(&traces_key, rlp_traces).await
    }

    pub async fn archive_hashes(
        &self,
        transactions: Vec<TransactionSigned>,
        receipts: Vec<ReceiptWithBloom>,
        traces: Vec<Vec<u8>>,
        tx_hashes: Vec<[u8; 32]>,
    ) -> Result<(), ArchiveError> {
        let mut handles = Vec::with_capacity(transactions.len());

        for i in 0..transactions.len() {
            let archive = Self::clone(self);
            let tx_hash = tx_hashes[i];

            let hash = TxHashTable::new(
                transactions[i].clone(),
                receipts[i].clone(),
                traces[i].clone(),
            );

            let handle = tokio::spawn(async move {
                let mut rlp_hash = Vec::new();
                hash.encode(&mut rlp_hash);
                let hash_key_suffix = hex::encode(tx_hash);
                let hash_key = format!("{}/{}", &archive.hash_table_prefix, hash_key_suffix);
                archive.kv.upload(&hash_key, rlp_hash).await
            });

            handles.push(handle);
        }

        let results = join_all(handles).await;

        for (idx, upload_result) in results.into_iter().enumerate() {
            if let Err(e) = upload_result {
                bail!("Failed to upload index: {}, {:?}", idx, e);
            }
        }

        Ok(())
    }
}

pub fn make_block(block_header: BlockHeader, transactions: Vec<TransactionSigned>) -> Block {
    Block {
        header: block_header.header,
        body: transactions,
        ..Default::default()
    }
}
