use alloy_primitives::U64;
use alloy_rlp::Encodable;
use alloy_rpc_client::ClientBuilder;
use alloy_rpc_types::Block;
use tokio::{fs::File, io::AsyncWriteExt};
use url::Url;

const RPC_URL: &str = "http://chi-001.devcore4.com:8545";

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = ClientBuilder::default().http(Url::parse(RPC_URL).unwrap());

    let raw_txs_dir = "eth_txs";
    std::fs::create_dir(raw_txs_dir).unwrap();

    let start_block = 19000000;
    let num_blocks = 100;

    // used to generate brotli dict
    let mut rlp_txs_file = File::create(format!("{raw_txs_dir}/rlp_txs"))
        .await
        .unwrap();

    for block_num in start_block..(start_block + num_blocks) {
        let block = client
            .request::<_, Block>("eth_getBlockByNumber", (U64::from(block_num), true))
            .await
            .unwrap();

        let mut raw_txs_file = File::create(format!("{raw_txs_dir}/{block_num}"))
            .await
            .unwrap();
        for tx in block.transactions.into_transactions().map(|tx| tx.inner) {
            let mut out = Vec::new();
            tx.encode(&mut out);

            rlp_txs_file.write_all(&out).await.unwrap();

            let hex_encoded = hex::encode(out) + "\n";
            raw_txs_file
                .write_all(hex_encoded.as_bytes())
                .await
                .unwrap();
        }
    }
}
