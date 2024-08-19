use std::{error::Error, str::FromStr};

use alloy_primitives::{Address, Bytes, FixedBytes, B256};
use clap::Parser;
use monad_secp::KeyPair;
use rand::{thread_rng, RngCore};
use reqwest::Url;
use reth_primitives::{
    keccak256, sign_message, AccessList, Transaction, TransactionKind, TransactionSigned, TxEip1559,
};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://localhost:8080")]
    rpc_url: Url,

    #[arg(
        long,
        default_value = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
    )]
    root_private_key: String,
}

#[derive(Clone)]
struct Client {
    url: Url,
    client: reqwest::Client,
}

impl Client {
    fn new(url: Url) -> Self {
        Self {
            url,
            client: reqwest::Client::new(),
        }
    }

    async fn rpc(&self, req: Value) -> Result<reqwest::Response, reqwest::Error> {
        let res = self
            .client
            .post(self.url.clone())
            .body(req.to_string())
            .send()
            .await;

        res
    }
}

#[derive(Deserialize)]
struct JsonResponse {
    pub jsonrpc: String,
    pub result: Option<Value>,
    pub error: Option<Value>,
    pub id: Value,
}

impl JsonResponse {
    fn get_result_u128(&self) -> u128 {
        let s = self.result.clone().unwrap();
        let s = s
            .as_str()
            .expect("result must be string")
            .trim_start_matches("0x");

        u128::from_str_radix(s, 16).unwrap()
    }
}

/// Represents an EOA and contains the private key for signing transactions.
/// Tracks the Nonce for the account
struct Account {
    priv_key: B256,
    address: Address,
    nonce: u64,
    balance: u128,
}

impl Account {
    fn new(private_key: String) -> Self {
        let pk = B256::from_str(&private_key).unwrap();
        Account::create_account(pk)
    }

    fn new_random() -> Self {
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        let pk = B256::from_slice(&bytes);
        Account::create_account(pk)
    }

    fn create_account(pk: B256) -> Self {
        let address = private_key_to_addr(pk);

        Self {
            priv_key: pk,
            address,
            nonce: 0,
            balance: 0,
        }
    }

    /// creates the raw RLP encoded bytes of the transaction
    fn create_raw_xfer_transaction(&self, dst: Address, nonce: u64, value: u128) -> Bytes {
        let transaction = Transaction::Eip1559(TxEip1559 {
            chain_id: 41454,
            nonce,
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 0,
            gas_limit: 21000,
            to: TransactionKind::Call(dst),
            value: value.into(),
            input: vec![].into(),
            access_list: AccessList::default(),
        });

        let hash = transaction.signature_hash();
        let signature =
            sign_message(self.priv_key, hash).expect("signing transaction should succeed");

        let signed_transaction =
            TransactionSigned::from_transaction_and_signature(transaction, signature);
        let raw_tx = signed_transaction.envelope_encoded();

        raw_tx
    }

    async fn update_nonce(&mut self, client: Client) {
        let req = json!(
            {
                "jsonrpc": "2.0",
                "method": "eth_getTransactionCount",
                "params": [self.address, "latest"],
                "id": 1,
            }
        );

        let res = client.rpc(req).await.expect("rpc not responding");
        let res = res
            .json::<JsonResponse>()
            .await
            .expect("json deser of response must not fail");

        let nonce = res.get_result_u128();
        self.nonce = nonce as u64;
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    Ok(())
}

fn private_key_to_addr(mut pk: B256) -> Address {
    let kp = KeyPair::from_bytes(pk.as_mut_slice()).unwrap();
    let pubkey_bytes = kp.pubkey().bytes();
    assert!(pubkey_bytes.len() == 65);
    let hash = keccak256(&pubkey_bytes[1..]);
    Address::from_slice(&hash[12..])
}

/// create a set of new accounts that have value transfered to them by a single root account
async fn create_source_accounts(
    root_account: Account,
    num_account: usize,
    value: u128,
) -> Vec<Account> {
    let dst_addrs: Vec<Account> = (0..num_account)
        .into_iter()
        .map(|_| Account::new_random())
        .collect();

    let raw_txns = dst_addrs
        .iter()
        .map(|dst| root_account.create_raw_xfer_transaction(dst.address, value))
        .collect::<Vec<_>>();

    todo!();
}

/// create a list of random accounts
fn create_rand_accounts() -> Vec<Account> {
    vec![]
}

async fn send_raw_txn() {}
async fn get_nonce() {}
async fn get_balance() {}

async fn transfer_test_1() {
    // create the root account
    // seed a set of source accounts
    //
    // use those source accounts to create batches of random accounts
}
