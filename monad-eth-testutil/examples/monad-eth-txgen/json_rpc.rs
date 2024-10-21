use alloy_rpc_client::ReqwestClient;
use eyre::{Context, Result};
use reth_primitives::{Address, Bytes, TransactionSigned, U256, U64};
use serde_json::Value;

pub trait JsonRpc {
    async fn send_raw_transaction_params(&self, tx: TransactionSigned) -> (&'static str, Bytes);
    async fn get_transaction_count(&self, addr: &Address) -> Result<u64>;
    async fn get_balance(&self, addr: &Address) -> Result<U256>;
}

impl JsonRpc for ReqwestClient {
    async fn get_transaction_count(&self, addr: &Address) -> Result<u64> {
        let addr = addr.to_string();
        let nonce = self
            .request::<_, U64>("eth_getTransactionCount", [&addr, "latest"])
            .await?;
        Ok(nonce.to())
    }

    async fn send_raw_transaction_params(&self, tx: TransactionSigned) -> (&'static str, Bytes) {
        ("eth_sendRawTransaction", tx.envelope_encoded())
    }

    async fn get_balance(&self, addr: &Address) -> Result<U256> {
        todo!()
    }
}
