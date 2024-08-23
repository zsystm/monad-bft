use std::{
    error::Error,
    iter::zip,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::{Address, Bytes, FixedBytes, B256};
use async_channel::{Receiver, Sender};
use clap::Parser;
use futures::{future::join_all, FutureExt, StreamExt};
use monad_secp::KeyPair;
use rand::{thread_rng, RngCore};
use reqwest::Url;
use reth_primitives::{
    keccak256, sign_message, AccessList, Transaction, TransactionKind, TransactionSigned, TxEip1559,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::join;
use tracing::warn;

const CARRIAGE_COST: usize = 21000 * 1000;
// This is the expected wait time after sending transactions before
// trying to refresh nonce and balance so that it is up to date
// NOTE: This is completely arbitrary, the time may vary depending on
// how fast execution runs on a specific node and network conditions
const EXECUTION_DELAY_WAIT_TIME: Duration = Duration::from_secs(5);

const TARGET_TPS: usize = 10000;

// FIXME ?
// Payload size with 868 raw txns: 262144
// Payload size with 869 raw txns: 262408 -> payload reached size limit ??
const BATCH_SIZE: usize = 500;

// each worker sends BATCH_SIZE transactions every second
const NUM_WORKERS: usize = TARGET_TPS / BATCH_SIZE;

// balance split from root account
// 2 splits with 1000 accounts per split = 1000^2 = 1_000_000 final accounts
const NUM_ACCOUNTS_PER_SPLIT: usize = 1000;
const NUM_SPLITS: usize = 2;

// number of batches allowed to buffer in the channels
const TX_CHANNEL_BUFFER: usize = 40;


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

#[derive(Debug, Deserialize)]
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

    // Local view of nonce and balance. Can be refreshed.
    // Initially assumed to be 0.
    // NOTE: If a transaction is sent to or from this account, refreshing immediately
    // could be incorrect because of execution delay.
    nonce: u64,
    // NOTE: If user updates this local view of balance after sending a transaction from this
    // account, make sure to periodically refresh to ensure exact gas fees are accounted for.
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
    fn create_raw_xfer_transaction(&mut self, dst: Address, value: u128) -> Bytes {
        let transaction = Transaction::Eip1559(TxEip1559 {
            chain_id: 41454,
            nonce: self.nonce,
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

        self.nonce += 1;

        raw_tx
    }

    // TODO: use getAccount to update both of these at the same time ?

    async fn refresh_nonce(&mut self, client: Client) {
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

        self.nonce = res.get_result_u128() as u64;
    }

    async fn refresh_balance(&mut self, client: Client) {
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

        self.balance = res.get_result_u128();
    }
}

fn private_key_to_addr(mut pk: B256) -> Address {
    let kp = KeyPair::from_bytes(pk.as_mut_slice()).unwrap();
    let pubkey_bytes = kp.pubkey().bytes();
    assert!(pubkey_bytes.len() == 65);
    let hash = keccak256(&pubkey_bytes[1..]);
    Address::from_slice(&hash[12..])
}

/// create a list of random accounts
fn create_rand_accounts(num_accounts: usize) -> Vec<Account> {
    (0..num_accounts)
        .into_iter()
        .map(|_| Account::new_random())
        .collect()
}

fn create_transfer_txns(
    root_account: &mut Account,
    dst_accounts: &Vec<Account>,
    transfer_amount: u128,
) -> Vec<Bytes> {
    // let num_targets = dst_accounts.len() + 1; // leave some left over token

    let raw_txns = dst_accounts
        .into_iter()
        .map(|dst| root_account.create_raw_xfer_transaction(dst.address, transfer_amount))
        .collect::<Vec<_>>();

    raw_txns
}

/// create a set of new accounts that have value transfered to them by a single root account
async fn create_random_source_accounts(
    root_account: &mut Account,
    num_accounts: usize,
    client: Client,
) -> Vec<Account> {
    let dst_accounts = create_rand_accounts(num_accounts);
    let raw_txns = create_transfer_txns(root_account, &dst_accounts, 100);

    let resp = send_batch_raw_txns(raw_txns, client).await;

    // id should match the index of the dst_addrs that was used in the originating request
    // id's that succeeded in transfer should be valid accounts to use going forward
    let mut results = vec![];
    for id in resp {
        results.push(dst_accounts[id].clone());
    }

    results
}

async fn send_one_to_many(
    src_account: &mut Account,
    dst_accounts: &Vec<Account>,
    value: u128,
    client: Client,
) {
    let starting_nonce = src_account.nonce;
    let starting_balance = src_account.balance;

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

    let raw_txns = dst_accounts
        .into_iter()
        .map(|dst| src_account.create_raw_xfer_transaction(dst.address, value))
        .collect::<Vec<_>>();
    let resp = send_batch_raw_txns(raw_txns, client).await;
    // TODO should we check how many txns actually went through?
}

// each src account will to all dst accounts
async fn send_many_to_many(
    src_accounts: &mut Vec<Account>,
    dst_accounts: &Vec<Account>,
    value: u128,
    client: Client,
) {
    let reqs: Vec<_> = src_accounts
        .iter_mut()
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

async fn send_raw_txn(raw_txn: Bytes, client: Client) -> usize {
    let req = json!(
        {
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",
            "params": [raw_txn],
            "id": 1,
        }
    );

    let res = client.rpc(req).await.expect("rpc not responding");
    let res = res
        .json::<JsonResponse>()
        .await
        .expect("json deser of response must not fail");

    res.id
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

    // the result of a batch request should be an array of responses for each req in the batch
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

async fn rpc_sender(txn_receiver: Receiver<Vec<Bytes>>, client: Client) {
    println!("spawned rpc sender");

    let mut interval = tokio::time::interval(Duration::from_millis(1000));
    loop {
        interval.tick().await;

        match txn_receiver.recv().await {
            Ok(next_batch) => {
                send_batch_raw_txns(next_batch, client.clone()).await;
            }
            Err(err) => {
                // channel is closed and no more txns exist
                println!("{}", err);
                break;
            }
        }
    }
}

async fn split_account_balance(
    mut root_account: Account,
    client: Client,
    txn_sender: Sender<Vec<Bytes>>,
) -> Vec<Account> {
    root_account.refresh_nonce(client.clone()).await;
    root_account.refresh_balance(client.clone()).await;

    let num_splits = NUM_ACCOUNTS_PER_SPLIT + 1; // leave some tokens in the root account
    let transfer_amount = if root_account.balance == 0 {
        // This could mean either:
        // 1. The transfer transaction sent to this account didn't make it through consensus, or
        // 2. Execution hasn't run far enough to reflect the accurate balance for this account

        // FIXME: this transaction will fail. should wait for balance.
        1000
    } else {
        root_account
            .balance
            .wrapping_sub(u128::try_from(CARRIAGE_COST * num_splits).unwrap())
            .wrapping_div(u128::try_from(num_splits).unwrap())
    };

    let num_batches = NUM_ACCOUNTS_PER_SPLIT.div_ceil(BATCH_SIZE);
    let mut new_accounts = Vec::new();
    for _ in 0..num_batches {
        let dst_accounts = create_rand_accounts(500);
        let txns = create_transfer_txns(&mut root_account, &dst_accounts, transfer_amount);
        let _ = txn_sender.send(txns).await.unwrap();

        new_accounts.extend(dst_accounts);
    }

    new_accounts
}

async fn create_final_accounts_from_root(
    root_account: Account,
    client: Client,
    txn_sender: Sender<Vec<Bytes>>,
) -> Vec<Account> {
    let mut execution_delay_interval = tokio::time::interval(EXECUTION_DELAY_WAIT_TIME);

    let mut accounts_to_split = vec![root_account];
    for _ in 0..NUM_SPLITS {
        // This interval tick is to wait for execution to catch up so that
        // nonce and balance are up to date when refreshed before splitting further
        execution_delay_interval.tick().await;

        let mut new_accounts = Vec::new();
        for account in accounts_to_split {
            let acc = split_account_balance(account, client.clone(), txn_sender.clone()).await;
            new_accounts.extend(acc);
        }

        // Keep splitting NUM_SPLITS times
        accounts_to_split = new_accounts;
    }

    assert!(txn_sender.close());

    accounts_to_split
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();

    let args = Args::parse();

    let client = Client::new(args.rpc_url);

    let (sender, receiver) = async_channel::bounded(TX_CHANNEL_BUFFER);

    let rpc_sender_handles =
        (0..NUM_WORKERS).map(|_| tokio::spawn(rpc_sender(receiver.clone(), client.clone())));

    let root_account = Account::new(args.root_private_key);
    let root_account_split_handle = tokio::spawn(create_final_accounts_from_root(root_account, client.clone(), sender.clone()));

    let s = join!(join_all(rpc_sender_handles), root_account_split_handle);

    let stop = Instant::now();
    let time = stop.duration_since(start).as_millis();

    println!("ran for {}", time);

    Ok(())
}
