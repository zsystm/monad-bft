use alloy_rpc_client::ReqwestClient;
use eyre::{eyre, Result};
use rand::Rng;
use reth_primitives::{Address, Bytes, TransactionSigned, U256, U64};
use tracing::{trace, warn};

use super::erc20::ERC20;

pub trait JsonRpc {
    async fn send_raw_transaction_params(&self, tx: TransactionSigned) -> (&'static str, Bytes);
    async fn get_transaction_count(&self, addr: &Address) -> Result<u64>;
    async fn get_balance(&self, addr: &Address) -> Result<U256>;

    async fn batch_get_balance(&self, addrs: &[String]) -> Result<Vec<Result<U256>>>;
    async fn batch_get_transaction_count(&self, addrs: &[String]) -> Result<Vec<Result<u64>>>;
    async fn batch_get_erc20_balance(
        &self,
        addrs: &[Address],
        erc20: ERC20,
    ) -> Result<Vec<Result<U256>>>;
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
        let addr = addr.to_string();
        self.request::<_, U256>("eth_getBalance", [&addr, "latest"])
            .await
            .map_err(Into::into)
    }

    async fn batch_get_balance(&self, addrs: &[String]) -> Result<Vec<Result<U256>>> {
        let now = std::time::Instant::now();
        let mut batch = self.new_batch();

        let futs: Vec<_> = addrs
            .iter()
            .map(|addr| {
                let params = [addr, "latest"];
                batch.add_call::<_, U256>("eth_getBalance", &params)
            })
            .collect::<Result<Vec<_>, _>>()?;

        batch.send().await?;

        let mut output = Vec::with_capacity(addrs.len());
        for fut in futs {
            output.push(fut.await.map_err(Into::into));
        }

        let elapsed_ms = now.elapsed().as_millis();
        trace!(num_addrs = addrs.len(), elapsed_ms, "batch_get_balance");

        Ok(output)
    }

    async fn batch_get_transaction_count(&self, addrs: &[String]) -> Result<Vec<Result<u64>>> {
        let now = std::time::Instant::now();
        let mut batch = self.new_batch();

        let futs: Vec<_> = addrs
            .iter()
            .map(|addr| {
                let params = [addr, "latest"];
                batch.add_call::<_, U64>("eth_getTransactionCount", &params)
            })
            .collect::<Result<Vec<_>, _>>()?;

        batch.send().await?;

        let mut output = Vec::with_capacity(addrs.len());
        for fut in futs {
            output.push(fut.await.map(|n| n.to()).map_err(Into::into));
        }

        let elapsed_ms = now.elapsed().as_millis();
        trace!(elapsed_ms, "batch_get_transaction_count");

        Ok(output)
    }

    async fn batch_get_erc20_balance(
        &self,
        addrs: &[Address],
        erc20: ERC20,
    ) -> Result<Vec<Result<U256>>> {
        if rand::thread_rng().gen_bool(0.0) {
            return Ok(addrs.iter().map(|_| Err(eyre!("whoops"))).collect());
        }

        let now = std::time::Instant::now();
        let mut batch = self.new_batch();

        let futs: Vec<_> = addrs
            .iter()
            .map(|addr| {
                let (method, params) = erc20.balance_of(*addr);
                batch.add_call::<_, U256>(method, &params)
            })
            .collect::<Result<Vec<_>, _>>()?;

        batch.send().await?;

        let mut output = Vec::with_capacity(addrs.len());
        for fut in futs {
            output.push(fut.await.map_err(Into::into));
        }

        let elapsed_ms = now.elapsed().as_millis();
        warn!(
            num_addrs = addrs.len(),
            elapsed_ms, "batch_get_erc20_balance"
        );
        Ok(output)
    }
}
