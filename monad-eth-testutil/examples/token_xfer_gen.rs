use std::{
    borrow::{BorrowMut}, error::Error, str::FromStr, sync::{Arc}, time::{Duration, Instant}
};

use alloy_primitives::{Address, Bytes, B256};
use async_channel::{Receiver, Sender};
use clap::Parser;
use futures::{future::join_all};
use monad_secp::KeyPair;
use rand::{RngCore};
use reqwest::Url;
use reth_primitives::{
    keccak256, sign_message, AccessList, Transaction, TransactionKind, TransactionSigned, TxEip1559,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{join, sync::Mutex};
use tracing::warn;

const CARRIAGE_COST: usize = 21000 * 1000;
// This is the expected wait time after sending transactions before
// trying to refresh nonce and balance so that it is up to date
// NOTE: This is completely arbitrary, the time may vary depending on
// how fast execution runs on a specific node and network conditions
const EXECUTION_DELAY_WAIT_TIME: Duration = Duration::from_secs(5);

// Payload size with batch = 868 raw txns: 262144
// Payload size with batch = 869 raw txns: 262408 -> payload reached size limit ??
// IPC batch size limit = 500
const BATCH_SIZE: usize = 500;

// each rpc sender will send 1 batch of BATCH_SIZE transactions every RPC_SENDER_INTERVAL
const NUM_RPC_SENDERS: usize = 16;
const RPC_SENDER_INTERVAL: Duration = Duration::from_millis(800);

const EXPECTED_TPS: usize = NUM_RPC_SENDERS * BATCH_SIZE / RPC_SENDER_INTERVAL.as_millis() as usize;

// balance split from root account
// e.g. 2 splits with 1000 accounts per split = 1000^2 = 1_000_000 final accounts
// FIXME: This shoudn't exceed BATCH_SIZE for now. nonce issue.
const NUM_ACCOUNTS_PER_SPLIT: usize = 100;
const NUM_SPLITS: u32 = 3;
const NUM_FINAL_ACCOUNTS: usize = NUM_ACCOUNTS_PER_SPLIT.pow(NUM_SPLITS);

// number of batches allowed in the txn batches channel
const TX_BATCHES_CHANNEL_BUFFER: usize = 40;

static mut num_nonce_updates: usize = 0;

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

    num_txns_sent: u64,
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
            num_txns_sent: 0,
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
        self.num_txns_sent += 1;
        // TODO: Also update the expected balance of the receiver account

        raw_tx
    }

    async fn create_raw_xfer_transaction_checked(&mut self, dst: Address, value: u128, client: Client) -> Bytes {
        // Refresh nonce and balance every 20 txns sent
        if self.num_txns_sent % 20 == 0 {
            self.refresh_nonce(client.clone()).await;
            self.refresh_balance(client).await;
            
            unsafe { num_nonce_updates += 1; }
        }

        self.create_raw_xfer_transaction(dst, value)
    }

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
    dst_accounts: &mut [Account],
    transfer_amount: u128,
) -> Vec<Bytes> {
    let raw_txns = dst_accounts
        .into_iter()
        .map(|dst| root_account.create_raw_xfer_transaction(dst.address, transfer_amount))
        .collect::<Vec<_>>();

    raw_txns
}

// src account will transfer tokens to dst_accounts.
// number of dst_accounts capped at BATCH_SIZE
async fn send_one_to_many(
    src_account: &mut Account,
    dst_accounts: &mut [Account],
    value: u128,
    txn_sender: Sender<Vec<Bytes>>,
) {
    let starting_balance = src_account.balance;
    let num_receiver_accounts = dst_accounts.len().min(BATCH_SIZE);

    match value.checked_mul(CARRIAGE_COST as u128) {
        Some(v) => {
            let total_amount = v.checked_mul(num_receiver_accounts as u128).unwrap();
            if total_amount > starting_balance {
                warn!("source account does not have enough balance to complete this operation");
                return;
            }
        }
        None => {
            panic!("multiply overflow, does value need to be 256bit?");
        }
    }

    let raw_txns = dst_accounts[0..num_receiver_accounts]
        .iter()
        .map(|dst| src_account.create_raw_xfer_transaction(dst.address, value))
        .collect::<Vec<_>>();

    let _ = txn_sender.send(raw_txns).await.unwrap();

    // TODO should we check how many txns actually went through?
}

// each src account will to all dst accounts
// number of dst_accounts capped at BATCH_SIZE
async fn send_many_to_many(
    src_accounts: &mut [Account],
    dst_accounts: &mut [Account],
    value: u128,
    txn_sender: Sender<Vec<Bytes>>,
) {
    for src_account in src_accounts {
        send_one_to_many(src_account, dst_accounts, value, txn_sender.clone()).await;
    }
}

fn random_account_selector(accounts: &mut [Account]) -> &mut Account {
    let acc_index = rand::random::<usize>() % accounts.len();
    accounts[acc_index].borrow_mut()
}

// creates batches of transactions that send between pairs of accounts
// pairs are chosen according to the passed in selection functions
async fn send_between_rand_pairings(
    src_accounts: &mut [Account],
    dst_accounts: &mut [Account],
    value: u128,
    src_selection: impl Fn(&mut [Account]) -> &mut Account,
    dst_selection: impl Fn(&mut [Account]) -> &mut Account,
    txn_sender: Sender<Vec<Bytes>>,
    num_batches: usize,
    client: Client,
) {
    for _ in 0..num_batches {
        let mut raw_txns = Vec::new();
        for _ in 0..BATCH_SIZE {
            let src_account = src_selection(src_accounts);
            let dst_account = dst_selection(dst_accounts);

            raw_txns.push(src_account.create_raw_xfer_transaction_checked(dst_account.address, value, client.clone()).await);
        }

        let _ = txn_sender.send(raw_txns).await.unwrap();
    }
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

// single RPC sender worker.
// waits for a batch of transactions (every second) from the txn receiver and forwards to RPC
async fn rpc_sender(
    rpc_sender_id: usize,
    txn_batch_receiver: Receiver<Vec<Bytes>>,
    client: Client,
    tx_counter: Arc<Mutex<usize>>,
) {
    println!("spawned rpc sender id: {}", rpc_sender_id);

    // interval, not sleep
    let mut interval = tokio::time::interval(RPC_SENDER_INTERVAL);
    loop {
        interval.tick().await;

        match txn_batch_receiver.recv().await {
            Ok(next_batch) => {
                send_batch_raw_txns(next_batch, client.clone()).await;
                *(tx_counter.lock().await) += BATCH_SIZE;
            }
            Err(err) => {
                // channel is closed and no more txns exist
                println!("rpc sender: {}, err: {}", rpc_sender_id, err);
                break;
            }
        }
    }
}

async fn split_account_balance(
    mut account_to_split: Account,
    num_new_accounts: usize,
    client: Client,
    txn_sender: Sender<Vec<Bytes>>,
) -> Vec<Account> {
    account_to_split.refresh_nonce(client.clone()).await;
    account_to_split.refresh_balance(client.clone()).await;

    if account_to_split.balance == 0 {
        // If balance is expected to be non-zero, this error could mean either:
        // 1. The transfer transaction sent to this account didn't make it through consensus, or
        // 2. Execution hasn't run far enough to reflect the accurate balance for this account

        println!(
            "account {} has zero balance, expected > 0",
            account_to_split.address
        );
        // FIXME: should wait for balance instead of returning empty ?
        return Vec::new();
    }

    let num_splits = num_new_accounts + 10; // leave some tokens in the root account
    let transfer_amount = account_to_split
        .balance
        .wrapping_sub(u128::try_from(CARRIAGE_COST * num_splits).unwrap())
        .wrapping_div(u128::try_from(num_splits).unwrap());

    let mut new_accounts = create_rand_accounts(num_new_accounts);
    for dst_accounts in new_accounts.chunks_mut(BATCH_SIZE) {
        let txns_batch = create_transfer_txns(
            &mut account_to_split,
            dst_accounts,
            transfer_amount,
        );
        let _ = txn_sender.send(txns_batch).await.unwrap();
    };

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
            let acc = split_account_balance(account, NUM_ACCOUNTS_PER_SPLIT, client.clone(), txn_sender.clone()).await;
            new_accounts.extend(acc);
        }

        // Keep splitting NUM_SPLITS times
        accounts_to_split = new_accounts;
    }

    accounts_to_split
}

async fn start_random_tx_gen(
    root_account: Account,
    client: Client,
    txn_batch_sender: Sender<Vec<Bytes>>,
) {
    let mut final_accounts =
        create_final_accounts_from_root(root_account, client.clone(), txn_batch_sender.clone())
            .await;
    
    tokio::time::sleep(Duration::from_secs(5)).await;
    println!("final accounts created");

    let value = 10;
    // let num_slices = NUM_FINAL_ACCOUNTS / 10_000 = 100 slices
    // let num_pairs = num_slices / 2 = 50 pairs
    // 4,600 batches per pair = 230,000 total batches = 115,000,000 txns
    // at 8000 TPS, that will take 14,375 seconds ~ 3.9 hrs
    let num_batches_per_pair = 4_600;

    // Split final accounts into slices of 10,000 accounts
    let mut final_account_slices: Vec<&mut [Account]> = final_accounts.chunks_mut(10_000).collect();
    // Group the slices into pairs and send transactions between those slices.
    let random_tx_gen_futures = final_account_slices.chunks_mut(2).filter_map(|slice| {
        if slice.len() != 2 {
            return None;
        }

        let (slice_1, slice_2) = slice.split_at_mut(1);
        Some(send_between_rand_pairings(
            slice_1[0],
            slice_2[0],
            value,
            random_account_selector,
            random_account_selector,
            txn_batch_sender.clone(),
            num_batches_per_pair,
            client.clone(),
        ))
    });
    join_all(random_tx_gen_futures).await;

    assert!(txn_batch_sender.close());
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();

    let args = Args::parse();

    let client = Client::new(args.rpc_url);

    println!(
        "num rpc senders: {}. batch size: {}. expected TPS ~ {}",
        NUM_RPC_SENDERS, BATCH_SIZE, EXPECTED_TPS
    );

    let (txn_batch_sender, txn_batch_receiver) = async_channel::bounded(TX_BATCHES_CHANNEL_BUFFER);
    let tx_counter = Arc::new(Mutex::new(0));

    let rpc_sender_handles = (0..NUM_RPC_SENDERS).map(|id| {
        tokio::spawn(rpc_sender(
            id,
            txn_batch_receiver.clone(),
            client.clone(),
            tx_counter.clone(),
        ))
    });

    let root_account = Account::new(args.root_private_key);
    let root_account_split_handle = tokio::spawn(start_random_tx_gen(
        root_account,
        client.clone(),
        txn_batch_sender.clone(),
    ));

    let _ = join!(join_all(rpc_sender_handles), root_account_split_handle);

    let stop = Instant::now();
    let time = stop.duration_since(start).as_millis();

    println!("ran for {}. num txns: {:?}. num nonce updates: {:?}", time, tx_counter, unsafe { num_nonce_updates });

    Ok(())
}
