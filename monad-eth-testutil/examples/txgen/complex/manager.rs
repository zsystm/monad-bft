use futures::StreamExt;
use reth_rpc_types::Block;

use crate::{prelude::*, shared::blockstream::BlockStream};

/// Responsible for updating senders based off incoming committed txs
pub struct TxManager {
    txs: TxMap,
    accts: AcctMap,
    blockstream: BlockStream, // todo: make this a receiver of a broadcast channel
}

impl TxManager {
    pub async fn spawn(client: ReqwestClient) -> Result<(TxMap, AcctMap)> {
        let manager = Self::new(client).await?;
        let state = (Arc::clone(&manager.txs), Arc::clone(&manager.accts));
        tokio::spawn(manager.run());
        Ok(state)
    }

    pub async fn new(client: ReqwestClient) -> Result<Self> {
        Ok(Self {
            txs: Arc::new(DashMap::with_capacity(10_000 * 60)), // 10k/s * 1 minute
            accts: Arc::new(DashMap::with_capacity(100_000)),
            blockstream: BlockStream::new(client, Duration::from_millis(25), false).await?,
        })
    }

    pub async fn run(mut self) {
        while let Some(block) = self.blockstream.next().await {
            let block = match block {
                Ok(b) => b,
                Err(e) => {
                    error!("Blockstream error: {e}");
                    continue;
                }
            };

            for tx_hash in block.transactions.hashes() {
                if let None = self.commit_tx(&block, *tx_hash) {
                    continue;
                }
            }
        }
    }

    fn commit_tx(&self, block: &Block, tx_hash: TxHash) -> Option<()> {
        let mut tx_state = self.txs.get_mut(&tx_hash)?;

        // mark tx as committed
        tx_state.committed_block = block.header.number;

        let mut sender = self.accts.get_mut(&tx_state.from)?;

        // update sender
        sender.commit_tx(tx_hash, &self.txs)

        // todo: update `to` account
    }
}
