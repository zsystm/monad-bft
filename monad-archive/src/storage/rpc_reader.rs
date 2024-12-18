use std::str::FromStr;

use alloy_rpc_client::{ClientBuilder, ReqwestClient};
use reth_rpc_types::TransactionReceipt;

use crate::LatestKind;
use eyre::{bail, Result};
use reth_primitives::{ruint::Uint, Block, BlockHash, Receipt, ReceiptWithBloom, TxType};

use super::BlockDataReader;

#[derive(Clone)]
pub struct RpcReader {
    client: ReqwestClient,
}

impl RpcReader {
    pub fn new(s: &str) -> Self {
        Self {
            client: ClientBuilder::default()
                .http(url::Url::from_str(s).expect("Url failed to parse")),
        }
    }
}

impl BlockDataReader for RpcReader {
    fn get_bucket(&self) -> &str {
        "rpc-reader"
    }

    async fn get_latest(&self, _latest_kind: LatestKind) -> Result<u64> {
        self.client
            .request::<&[u8], u64>("eth_blockNumber", &[])
            .await
            .map_err(Into::into)
    }

    async fn get_block_by_number(&self, block_num: u64) -> Result<Block> {
        self.client
            .request::<_, Block>("eth_getBlockByNumber", &[block_num])
            .await
            .map_err(Into::into)
    }

    async fn get_block_by_hash(&self, block_hash: BlockHash) -> Result<Block> {
        self.client
            .request::<_, Block>("eth_getBlockByHash", &[block_hash])
            .await
            .map_err(Into::into)
    }

    async fn get_block_receipts(&self, block_number: u64) -> Result<Vec<ReceiptWithBloom>> {
        let rxs = self
            .client
            .request::<_, Vec<TransactionReceipt>>("eth_getBlockReceipts", &[block_number])
            .await?;

        let convert_tx_type = |ident: u8| {
            Ok(match ident {
                0 => TxType::Legacy,
                1 => TxType::EIP2930,
                2 => TxType::EIP1559,
                3 => TxType::EIP4844,
                _ => bail!("unexpected transaction type {}", ident),
            })
        };

        let convert_receipt = |rx: TransactionReceipt| -> Result<ReceiptWithBloom> {
            Ok(ReceiptWithBloom {
                bloom: rx.logs_bloom,
                receipt: Receipt {
                    tx_type: convert_tx_type(rx.transaction_type.to::<u8>())?,
                    success: rx.status_code == Some(Uint::from(1)),
                    cumulative_gas_used: rx.cumulative_gas_used.to::<u64>(),
                    logs: rx
                        .logs
                        .into_iter()
                        .map(|log| reth_primitives::Log {
                            address: log.address,
                            topics: log.topics,
                            data: log.data,
                        })
                        .collect(),
                },
            })
        };

        rxs.into_iter().map(convert_receipt).collect()
    }

    async fn get_block_traces(&self, _block_number: u64) -> Result<Vec<Vec<u8>>> {
        // FIXME: implement
        unimplemented!("Unclear how to get raw traces from rpc server")
    }
}
