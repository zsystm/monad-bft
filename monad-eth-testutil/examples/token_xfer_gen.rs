use std::{error::Error, iter::zip, str::FromStr};

use alloy_primitives::{Address, Bytes, FixedBytes, B256};
use clap::Parser;
use futures::future::join_all;
use monad_secp::KeyPair;
use rand::{thread_rng, RngCore};
use reqwest::Url;
use reth_primitives::{
    keccak256, sign_message, AccessList, Transaction, TransactionKind, TransactionSigned, TxEip1559,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::warn;

const CARRIAGE_COST: usize = 21000 * 1000;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://localhost:8080")]
    rpc_url: Url,

    /// private key for initial account to source tokens to other accounts
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
    pub id: usize,
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
#[derive(Clone)]
struct Account {
    priv_key: B256,
    address: Address,

    // Some(_) for these values means we know what the value in state is and have that up to date
    // value here.
    // None means it's not known and needs to be read from state.
    nonce: Option<u64>,
    balance: Option<u128>,
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
            nonce: None,
            balance: None,
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

    async fn get_nonce(&self, client: Client) -> u64 {
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

        res.get_result_u128() as u64
    }

    async fn get_balance(&self, client: Client) -> u128 {
        let req = json!(
            {
                "jsonrpc": "2.0",
                "method": "eth_getBalance",
                "params": [self.address, "latest"],
                "id": 1,
            }
        );

        let res = client.rpc(req).await.expect("rpc not responding");
        let res = res
            .json::<JsonResponse>()
            .await
            .expect("json deser of response must not fail");

        res.get_result_u128()
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let root_account = Account::new(args.root_private_key);
    let client = Client::new(args.rpc_url);

    let source_accounts = create_random_source_accounts(root_account, 1000, client);

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
async fn create_random_source_accounts(
    root_account: Account,
    num_account: usize,
    client: Client,
) -> Vec<Account> {
    let dst_addrs = create_rand_accounts(num_account);

    let starting_nonce = root_account.get_nonce(client.clone()).await;
    let starting_balance = root_account.get_balance(client.clone()).await;
    let num_targets = dst_addrs.len() + 1; // leave some left over token
    let xfer_amount = starting_balance
        .wrapping_sub(u128::try_from(CARRIAGE_COST * num_targets).unwrap())
        .wrapping_div(u128::try_from(num_targets).unwrap());

    let raw_txns = zip(starting_nonce.., &dst_addrs)
        .into_iter()
        .map(|(nonce, dst)| {
            root_account.create_raw_xfer_transaction(dst.address, nonce, xfer_amount)
        })
        .collect::<Vec<_>>();

    let resp = send_batch_raw_txns(raw_txns, client).await;

    // id should match the index of the dst_addrs that was used in the originating request
    // id's that succeeded in transfer should be valid accounts to use going forward
    let mut results = vec![];
    for id in resp {
        results.push(dst_addrs[id].clone());
    }

    results
}

async fn send_one_to_many(
    src_account: &Account,
    dst_accounts: &Vec<Account>,
    value: u128,
    client: Client,
) {
    let starting_nonce = src_account.get_nonce(client.clone()).await;
    let starting_balance = src_account.get_balance(client.clone()).await;

    match value.checked_mul(CARRIAGE_COST as u128) {
        Some(v) => {
            let total_amount = v.checked_mul(dst_accounts.len() as u128).unwrap();
            if total_amount > starting_balance {
                warn!("source account does not have enough balance to complete this operation");
                return;
            }
        }
        None => {
            panic!("multiply overflow, does value need to be 256bit?");
        }
    }

    let raw_txns = zip(starting_nonce.., dst_accounts)
        .into_iter()
        .map(|(nonce, dst)| src_account.create_raw_xfer_transaction(dst.address, nonce, value))
        .collect::<Vec<_>>();
    let resp = send_batch_raw_txns(raw_txns, client).await;
    // TODO should we check how many txns actually went through?
}

// each src account will to all dst accounts
async fn send_many_to_many(
    src_accounts: &Vec<Account>,
    dst_accounts: &Vec<Account>,
    value: u128,
    client: Client,
) {
    let reqs: Vec<_> = src_accounts
        .iter()
        .map(|src| send_one_to_many(src, dst_accounts, value, client.clone()))
        .collect();

    join_all(reqs).await;
}

// creates a batch of transactions that send between pairs of accounts
// pairs are chosen according to the passed in selection functions
async fn send_between_rand_pairings(
    src_accounts: Vec<Account>,
    dst_accounts: Vec<Account>,
    value: u128,
    src_selection: impl Fn(&[Account]) -> &Account,
    dst_selection: impl Fn(&[Account]) -> &Account,
    batch_size: usize,
    client: Client,
) {
}

/// create a list of random accounts
fn create_rand_accounts(num_accounts: usize) -> Vec<Account> {
    (0..num_accounts)
        .into_iter()
        .map(|_| Account::new_random())
        .collect()
}

// sends the requests in a batch list in the order of the raw_txns input vector. The index of each
// request is used as the id field in the batch list. The return value is a vector of the indices
// that returned success
async fn send_batch_raw_txns(raw_txns: Vec<Bytes>, client: Client) -> Vec<usize> {
    // TODO, not sure if there a batch size limit so may need to make multiple batches
    // enumerate the batch calls with id so that we can match with the array of responses that the
    // server returns. The list of responses
    let json_values = raw_txns
        .iter()
        .enumerate()
        .map(|(id, txn)| {
            json!({
                    "jsonrpc": "2.0",
                    "method": "eth_sendRawTransaction",
                    "params": [txn],
                    "id": id,
            })
        })
        .collect::<Vec<Value>>();

    let batch_req = serde_json::Value::Array(json_values);

    // the result of a batch request should be an array of respsonses for each req in the batch
    // list, but the ordering does not need to be maintained -- the id field is used to identify
    // which response goes with which request
    let res = client.rpc(batch_req).await.expect("rpc not responding");
    res.json::<Vec<JsonResponse>>()
        .await
        .expect("json deser of response must not fail")
        .iter()
        .filter_map(|resp| resp.result.as_ref().map(|_| resp.id))
        .collect()
}

async fn update_nonces(accounts: &mut Vec<Account>, client: Client) {
    let json_values = accounts
        .iter()
        .enumerate()
        .map(|(id, acc)| {
            json!({
                    "jsonrpc": "2.0",
                    "method": "eth_getTransactionCount",
                    "params": [acc.address, "latest"],
                    "id": id,
            })
        })
        .collect::<Vec<Value>>();
    let batch_req = serde_json::Value::Array(json_values);
    let res = client.rpc(batch_req).await.expect("rpc not responding");
    let updates = res
        .json::<Vec<JsonResponse>>()
        .await
        .expect("json deser of response must not fail")
        .iter()
        .filter_map(|resp| resp.result.as_ref().map(|_| resp.id))
        .collect::<Vec<_>>();

    //TODO, use updates to update accounts' nonces
}
