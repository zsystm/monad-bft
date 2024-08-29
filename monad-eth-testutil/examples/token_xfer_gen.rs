use core::num;
use std::{
    borrow::BorrowMut,
    error::Error,
    fs::File,
    io::{BufRead, BufReader, BufWriter, LineWriter, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use alloy_primitives::{Address, Bytes, B256};
use async_channel::{Receiver, Sender};
use clap::Parser;
use futures::future::join_all;
use monad_secp::KeyPair;
use rand::RngCore;
use reqwest::Url;
use reth_primitives::{
    keccak256, sign_message, AccessList, Transaction, TransactionKind, TransactionSigned, TxEip1559,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{
    join,
    time::{Duration, Instant, MissedTickBehavior},
};

const CARRIAGE_COST: usize = 21000 * 1000;
// This is the expected wait time after sending transactions before
// trying to refresh nonce and balance so that it is up to date
// NOTE: This is completely arbitrary, the time may vary depending on
// how fast execution runs on a specific node and network conditions
const EXECUTION_DELAY_WAIT_TIME: Duration = Duration::from_secs(5);

// Payload size with batch = 868 raw txns: 262144
// Payload size with batch = 869 raw txns: 262408 -> payload reached size limit
// IPC batch size limit = 500
const TXN_BATCH_SIZE: usize = 500;

// each rpc sender will send 1 batch of BATCH_SIZE transactions every RPC_SENDER_INTERVAL
const NUM_RPC_SENDERS: usize = 16;

// number of batches of transactions allowed in the txn batches channel
const TX_BATCHES_CHANNEL_BUFFER: usize = 40;

// number of batches of accounts allowed in the pipeline
const ACCOUNTS_BATCHES_CHANNEL_BUFFER: usize = 40;

// number of accounts to be created per accounts batch
// each accounts batch is sent into the pipeline
const NUM_ACCOUNTS_PER_BATCH: usize = 500_000;
// number of txn batches to generate per accounts batch
// 4,000 batches = 2,000,000 txns per accounts batch in pipeline
const NUM_TXN_BATCHES_PER_ACCOUNTS_BATCH: usize = 4_000;
// e.g. with 10,000,000 final accounts, there will be 20 accounts batches
// if each batch sends 2,000,000 txns, total txns count is 40,000,000 txns

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

    /// if provided, private keys will be extracted from this file and used as final accounts
    /// private keys
    #[arg(long)]
    priv_keys_file: Option<String>,

    /// Initial splitting of root account balance occurs in batches. Balance of root account
    /// is split into multiple accounts and balance of newly created accounts are split further.
    /// May create more accounts than num_final_accounts if not exactly divisible by BATCH_SIZE
    #[arg(long, default_value_t = 1_000_000)]
    num_final_accounts: u64,

    /// interval at which each RPC sender sends a batch of transaction to RPC (in milliseconds)
    #[arg(long, default_value_t = 1000)]
    rpc_sender_interval_ms: u64,
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

    async fn refresh_nonce(&mut self, client: Client) {
        let req = json!(
            {
                "jsonrpc": "2.0",
                "method": "eth_getTransactionCount",
                "params": [self.address, "latest"],
                "id": 1,
            }
        );

        let res = match client.rpc(req).await {
            Ok(res) => res,
            Err(err) => {
                println!("rpc not responding for refresh nonce: {}", err);
                return;
            }
        };
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

        let res = match client.rpc(req).await {
            Ok(res) => res,
            Err(err) => {
                println!("rpc not responding for refresh balance: {}", err);
                return;
            }
        };
        let res = res
            .json::<JsonResponse>()
            .await
            .expect("json deser of response must not fail");

        self.balance = res.get_result_u128();
    }
}

async fn batch_refresh_accounts(accounts: &mut [Account], client: Client) {
    assert!(accounts.len() == TXN_BATCH_SIZE);

    // create BATCH_SIZE requests for nonce updates
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
    let nonces_res_future = client.rpc(batch_req);

    // create BATCH_SIZE requests for balance updates
    let json_values = accounts
        .iter()
        .enumerate()
        .map(|(id, acc)| {
            json!({
                    "jsonrpc": "2.0",
                    "method": "eth_getBalance",
                    "params": [acc.address, "latest"],
                    "id": id,
            })
        })
        .collect::<Vec<Value>>();
    let batch_req = serde_json::Value::Array(json_values);
    let balances_res_future = client.rpc(batch_req);
    let (nonces_res, balances_res) = join!(nonces_res_future, balances_res_future);

    let nonces_res = nonces_res
        .expect("rpc not responding")
        .json::<Vec<JsonResponse>>()
        .await
        .expect("json deser of response must not fail");
    let balances_res = balances_res
        .expect("rpc not responding")
        .json::<Vec<JsonResponse>>()
        .await
        .expect("json deser of response must not fail");

    for single_res in nonces_res {
        accounts[single_res.id].nonce = single_res.get_result_u128() as u64;
    }
    for single_res in balances_res {
        accounts[single_res.id].balance = single_res.get_result_u128();
    }
}

pub struct AccountsRefreshContext {
    accounts_to_refresh: Vec<Account>,
    ready_sender: Sender<Vec<Account>>,
}

async fn accounts_refresher(
    refresh_context_receiver: Receiver<AccountsRefreshContext>,
    client: Client,
) {
    println!("starting account refresher");
    let mut num_refreshes = 0;
    loop {
        // receive a refresh context
        // if err, tx gen has completed and no more accounts to refresh. end loop
        match refresh_context_receiver.recv().await {
            Ok(mut refresh_context) => {
                // received some accounts to refresh

                num_refreshes += 1;
                println!(
                    "accounts refresher: received accounts to refresh. batch {}",
                    num_refreshes
                );
                let start = Instant::now();

                // wait for execution delay before refreshing
                tokio::time::sleep(EXECUTION_DELAY_WAIT_TIME).await;

                // refresh nonce and balance of every account
                for accounts in refresh_context
                    .accounts_to_refresh
                    .chunks_mut(TXN_BATCH_SIZE)
                {
                    batch_refresh_accounts(accounts, client.clone()).await;
                }

                // send the refreshed accounts through the ready sender
                refresh_context
                    .ready_sender
                    .send(refresh_context.accounts_to_refresh)
                    .await
                    .expect("ready sender error");

                let time_taken_ms = Instant::now().duration_since(start).as_millis();
                println!(
                    "accounts refresher: sent refreshed accounts. batch {}. took {}ms",
                    num_refreshes, time_taken_ms
                );
            }
            Err(err) => {
                println!("accounts refresher receiver channel: {}", err);
                break;
            }
        }
    }
    println!("stopped account refresher");
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
    let num_receiver_accounts = dst_accounts.len().min(TXN_BATCH_SIZE);

    match value.checked_mul(CARRIAGE_COST as u128) {
        Some(v) => {
            let total_amount = v.checked_mul(num_receiver_accounts as u128).unwrap();
            if total_amount > starting_balance {
                println!("source account does not have enough balance to complete this operation");
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

fn uniform_account_selector(accounts: &mut [Account]) -> &mut Account {
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
) {
    for _ in 0..num_batches {
        let mut raw_txns = Vec::new();
        for _ in 0..TXN_BATCH_SIZE {
            let src_account = src_selection(src_accounts);
            let dst_account = dst_selection(dst_accounts);

            raw_txns.push(src_account.create_raw_xfer_transaction(dst_account.address, value));
        }

        let _ = txn_sender.send(raw_txns).await.unwrap();
    }
}

async fn generate_uniform_rand_pairings(
    accounts_ready_receiver: Receiver<Vec<Account>>,
    accounts_completed_sender: Sender<Vec<Account>>,
    num_txns_batches: usize,
    txn_batch_sender: Sender<Vec<Bytes>>,
) {
    let mut num_ready_batches = 0;
    loop {
        match accounts_ready_receiver.recv().await {
            Ok(mut accounts_slice) => {
                // received accounts slice to generate transactions with
                num_ready_batches += 1;
                println!(
                    "generate uniform rand pairings: received refreshed accounts. batch {}",
                    num_ready_batches
                );

                let num_accounts_in_slice = accounts_slice.len();
                let (slice_1, slice_2) = accounts_slice.split_at_mut(num_accounts_in_slice / 2);
                let transfer_value = 10;
                send_between_rand_pairings(
                    slice_1,
                    slice_2,
                    transfer_value,
                    uniform_account_selector,
                    uniform_account_selector,
                    txn_batch_sender.clone(),
                    num_txns_batches,
                )
                .await;

                let _ = accounts_completed_sender.send(accounts_slice).await;
                println!(
                    "generate uniform rand pairings: sent completed accounts. batch {}",
                    num_ready_batches
                );
            }
            Err(err) => {
                // channel is closed and no more accounts exist
                println!("generate uniform rand pairings channel: {}", err);
                break;
            }
        }
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
    let res = match client.rpc(batch_req).await {
        Ok(res) => res,
        Err(err) => {
            println!("rpc not responding for send batch raw txns: {}", err);
            return Vec::new();
        }
    };
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
    rpc_sender_interval: Duration,
    txn_batch_receiver: Receiver<Vec<Bytes>>,
    client: Client,
    tx_counter: Arc<AtomicUsize>,
) {
    println!("spawned rpc sender id: {}", rpc_sender_id);

    // interval, not sleep
    let mut interval = tokio::time::interval(rpc_sender_interval);
    // to ensure TPS doesn't spike if there is a gap between tx gen
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        interval.tick().await;

        match txn_batch_receiver.recv().await {
            Ok(next_batch) => {
                // TODO: Use the response to update nonces
                send_batch_raw_txns(next_batch, client.clone()).await;
                tx_counter.fetch_add(TXN_BATCH_SIZE, Ordering::SeqCst);
            }
            Err(err) => {
                // channel is closed and no more txns exist
                println!("rpc sender: {}: {}", rpc_sender_id, err);
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
    for dst_accounts in new_accounts.chunks_mut(TXN_BATCH_SIZE) {
        let txns_batch = create_transfer_txns(&mut account_to_split, dst_accounts, transfer_amount);
        let _ = txn_sender.send(txns_batch).await.unwrap();
    }

    new_accounts
}

async fn create_final_accounts_from_root(
    root_account: Account,
    mut num_final_accounts: usize,
    client: Client,
    txn_sender: Sender<Vec<Bytes>>,
) -> Vec<Account> {
    let mut execution_delay_interval = tokio::time::interval(EXECUTION_DELAY_WAIT_TIME);

    // Calculates the number of splits based on BATCH_SIZE.
    // e.g. For 1,000,000 final accounts and BATCH_SIZE = 500, root accounts will be split into 4
    // accounts, followed by 500 per account, followed by 500 per account.
    // Total accounts created = 4 * 500 * 500 = 1,000,000
    let mut split_levels = Vec::new();
    while num_final_accounts > TXN_BATCH_SIZE {
        split_levels.push(TXN_BATCH_SIZE);
        // This may create more accounts than num_final_accounts
        num_final_accounts = num_final_accounts.div_ceil(TXN_BATCH_SIZE);
    }
    split_levels.push(num_final_accounts);

    let mut accounts_to_split = vec![root_account];
    for (split_level, num_new_accounts_per_split) in split_levels.iter().rev().enumerate() {
        // This interval tick is to wait for execution to catch up so that
        // nonce and balance are up to date when refreshed before splitting further
        execution_delay_interval.tick().await;

        let expected_num_new_accounts = accounts_to_split.len() * num_new_accounts_per_split;
        println!(
            "splitting {} account(s) to {} accounts. level {}",
            accounts_to_split.len(),
            expected_num_new_accounts,
            split_level + 1
        );

        let mut new_accounts = Vec::new();
        for account in accounts_to_split {
            let acc = split_account_balance(
                account,
                *num_new_accounts_per_split,
                client.clone(),
                txn_sender.clone(),
            )
            .await;
            new_accounts.extend(acc);
        }

        // Keep splitting new accounts
        accounts_to_split = new_accounts;
    }

    accounts_to_split
}

/// Splits the final accounts into chunks
/// 10 batches 1,000,000 accounts would return a vec of 100,000 accounts per batch
fn split_final_accounts_into_batches(
    mut final_accounts: Vec<Account>,
    num_accounts_per_batch: usize,
) -> Vec<Vec<Account>> {
    let num_final_accounts = final_accounts.len();

    let mut accounts_batches = Vec::new();
    let num_batches = num_final_accounts / num_accounts_per_batch;
    for i in 1..num_batches {
        accounts_batches
            .push(final_accounts.split_off(num_final_accounts - (i * num_accounts_per_batch)))
    }
    accounts_batches.push(final_accounts);

    accounts_batches
}

async fn make_final_accounts(
    root_account: Account,
    num_final_accounts: usize,
    priv_keys_file_path: Option<String>,
    client: Client,
    txn_batch_sender: Sender<Vec<Bytes>>,
) -> Vec<Account> {
    let final_accounts = if let Some(priv_keys_file_path) = priv_keys_file_path {
        println!("loading private keys from {}", priv_keys_file_path);

        let mut final_accounts = Vec::new();

        let file = File::open(priv_keys_file_path).expect("private keys file should exist");
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let pk = line.expect("each line should be a private key");
            final_accounts.push(Account::new(pk));
        }

        final_accounts
    } else {
        let final_accounts = create_final_accounts_from_root(
            root_account,
            num_final_accounts,
            client.clone(),
            txn_batch_sender.clone(),
        )
        .await;

        let mut file = BufWriter::new(File::create("priv_keys.txt").expect("no error"));
        for account in final_accounts.iter() {
            file.write(account.priv_key.to_string().as_bytes()).unwrap();
            file.write(b"\n").unwrap();
        }
        file.flush().unwrap();

        final_accounts
    };

    final_accounts
}

async fn start_random_tx_gen(
    root_account: Account,
    num_final_accounts: usize,
    priv_keys_file_path: Option<String>,
    client: Client,
    txn_batch_sender: Sender<Vec<Bytes>>,
) {
    let final_accounts = make_final_accounts(
        root_account,
        num_final_accounts,
        priv_keys_file_path,
        client.clone(),
        txn_batch_sender.clone(),
    )
    .await;
    let num_final_accounts = final_accounts.len();
    println!("{} final accounts created", num_final_accounts);

    let (refresh_context_sender, refresh_context_receiver) =
        async_channel::bounded(ACCOUNTS_BATCHES_CHANNEL_BUFFER);
    let accounts_refresher_handle =
        tokio::spawn(accounts_refresher(refresh_context_receiver, client.clone()));

    let (generate_rand_pairings_sender, generate_rand_pairings_receiver) =
        async_channel::bounded(ACCOUNTS_BATCHES_CHANNEL_BUFFER);
    let (accounts_completed_sender, accounts_completed_receiver) =
        async_channel::bounded(ACCOUNTS_BATCHES_CHANNEL_BUFFER);

    let tx_gen_rand_pairings_handle = tokio::spawn(generate_uniform_rand_pairings(
        generate_rand_pairings_receiver,
        accounts_completed_sender,
        NUM_TXN_BATCHES_PER_ACCOUNTS_BATCH,
        txn_batch_sender.clone(),
    ));

    let accounts_batches =
        split_final_accounts_into_batches(final_accounts, NUM_ACCOUNTS_PER_BATCH);
    let num_batches = accounts_batches.len();

    for accounts in accounts_batches {
        refresh_context_sender
            .send(AccountsRefreshContext {
                accounts_to_refresh: accounts,
                ready_sender: generate_rand_pairings_sender.clone(),
            })
            .await
            .unwrap();
    }

    let _completed_batches = join_all((0..num_batches).map(|_| async {
        accounts_completed_receiver
            .recv()
            .await
            .expect("completed batches channel error")
    }))
    .await;

    assert!(refresh_context_sender.close());
    assert!(generate_rand_pairings_sender.close());

    println!("closed refresher and ready channel");

    let refresher_result = accounts_refresher_handle.await;
    assert!(refresher_result.is_ok());

    let tx_gen_result = tx_gen_rand_pairings_handle.await;
    assert!(tx_gen_result.is_ok());

    assert!(txn_batch_sender.close());
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();

    let args = Args::parse();

    let client = Client::new(args.rpc_url);

    let rpc_sender_interval = Duration::from_millis(args.rpc_sender_interval_ms);
    let expected_tps =
        (NUM_RPC_SENDERS * TXN_BATCH_SIZE) as f64 / rpc_sender_interval.as_secs_f64();
    println!(
        "num rpc senders: {}. batch size: {}. expected TPS ~ {}",
        NUM_RPC_SENDERS, TXN_BATCH_SIZE, expected_tps
    );

    let (txn_batch_sender, txn_batch_receiver) = async_channel::bounded(TX_BATCHES_CHANNEL_BUFFER);
    let tx_counter = Arc::new(AtomicUsize::new(0));

    let rpc_sender_handles = (0..NUM_RPC_SENDERS).map(|id| {
        tokio::spawn(rpc_sender(
            id,
            rpc_sender_interval,
            txn_batch_receiver.clone(),
            client.clone(),
            tx_counter.clone(),
        ))
    });

    let root_account = Account::new(args.root_private_key);
    let num_final_accounts = args.num_final_accounts as usize;
    let start_tx_gen_handle = tokio::spawn(start_random_tx_gen(
        root_account,
        num_final_accounts,
        args.priv_keys_file,
        client.clone(),
        txn_batch_sender.clone(),
    ));

    let _ = join!(join_all(rpc_sender_handles), start_tx_gen_handle);

    let stop = Instant::now();
    let time = stop.duration_since(start).as_millis();

    println!("ran for {}. num txns: {:?}.", time, tx_counter,);

    Ok(())
}
